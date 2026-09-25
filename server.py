# server.py
import argparse
import queue
import threading
import time
import uuid
from concurrent import futures

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

import taskflow_pb2
import taskflow_pb2_grpc
from interceptors import AuthInterceptor, LoggingInterceptor

_STOP = object()  # sentinelle : sert à débloquer q.get() (voir TODO 8)


def _now() -> Timestamp:
    ts = Timestamp()
    ts.GetCurrentTime()
    return ts


def _status_name(value: int) -> str:
    return taskflow_pb2.TaskStatus.Name(value)   # 2 -> "DONE"


class TaskFlowService(taskflow_pb2_grpc.TaskFlowServicer):
    def __init__(self, slow: bool = False):
        # Modèle interne : dict id -> dict Python. On construit un message
        # protobuf NEUF à chaque réponse (voir self._to_pb) : on ne renvoie
        # jamais un objet partagé qu'un autre thread pourrait modifier.
        self._tasks = {}
        self._lock = threading.RLock()
        # une queue d'événements par abonné
        self._subscribers = []          # liste de (username, event_types, Queue)
        self._subs_lock = threading.Lock()
        self._slow = slow               # bonus B1

    # ---------- TODO(1) ----------
    def CreateTask(self, request, context):
        if not request.title:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "title is required")
        if not request.created_by:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "created_by is required")

        if self._slow:                  # bonus B1 : provoque DEADLINE_EXCEEDED
            time.sleep(5)

        task = {
            "id": str(uuid.uuid4()),
            "title": request.title,
            "description": request.description,
            "status": taskflow_pb2.TODO,
            "assigned_to": request.assigned_to,
            "created_by": request.created_by,
            "created_at": _now(),
            "comments": [],
        }
        with self._lock:
            self._tasks[task["id"]] = task
            response = taskflow_pb2.CreateTaskResponse(task=self._to_pb(task))

        self._publish("CREATED", task["id"], task["created_by"],
                      f"{task['created_by']} a créé « {task['title']} »")
        return response

    # ---------- TODO(2) ----------
    def GetTask(self, request, context):
        with self._lock:
            task = self._get_or_abort(request.id, context)
            return self._to_pb(task)

    # ---------- TODO(3) ----------
    def ListTasks(self, request, context):
        with self._lock:
            tasks = [self._to_pb(t) for t in self._tasks.values()]

        # yield HORS du verrou : un client lent ne doit pas bloquer les autres RPC
        for task in tasks:
            if request.HasField("status_filter") and task.status != request.status_filter:
                continue
            if request.HasField("assigned_filter") and task.assigned_to != request.assigned_filter:
                continue
            yield task

    # ---------- TODO(4) ----------
    def UpdateStatus(self, request, context):
        with self._lock:
            task = self._get_or_abort(request.id, context)
            if task["status"] == request.new_status:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    f"task already in status {_status_name(task['status'])}")
            if task["status"] == taskflow_pb2.DONE:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                              "cannot reopen a DONE task")
            task["status"] = request.new_status
            response = self._to_pb(task)

        self._publish(
            "STATUS_CHANGED", task["id"], request.requested_by,
            f"{request.requested_by} a passé « {task['title']} » "
            f"à {_status_name(request.new_status)}")
        return response

    # ---------- TODO(5) ----------
    def AssignTask(self, request, context):
        if not request.new_assignee:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          "new_assignee is required")
        with self._lock:
            task = self._get_or_abort(request.id, context)
            if task["assigned_to"] == request.new_assignee:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    f"task already assigned to {request.new_assignee}")
            task["assigned_to"] = request.new_assignee
            response = self._to_pb(task)

        self._publish(
            "ASSIGNED", task["id"], request.requested_by,
            f"{request.requested_by} a assigné « {task['title']} » "
            f"à {request.new_assignee}")
        return response

    # ---------- TODO(6) ----------
    def AddComment(self, request, context):
        if not request.text:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "text is required")
        with self._lock:
            task = self._get_or_abort(request.id, context)
            task["comments"].append({
                "author": request.author,
                "text": request.text,
                "created_at": _now(),
            })
            response = self._to_pb(task)

        self._publish("COMMENTED", task["id"], request.author,
                      f"{request.author} a commenté « {task['title']} »")
        return response

    # ---------- TODO(7) ----------
    def DeleteTask(self, request, context):
        with self._lock:
            task = self._get_or_abort(request.id, context)
            if task["created_by"] != request.requested_by:
                context.abort(
                    grpc.StatusCode.PERMISSION_DENIED,
                    f"only {task['created_by']} can delete this task")
            del self._tasks[request.id]

        self._publish("DELETED", task["id"], request.requested_by,
                      f"{request.requested_by} a supprimé « {task['title']} »")
        return taskflow_pb2.Empty()

    # ---------- TODO(8) ----------
    def Subscribe(self, request, context):
        # PAS un générateur : l'abonné doit être inscrit dès l'appel, et le
        # callback de fin de RPC doit être posé avant de bloquer sur q.get().
        q = queue.Queue()
        entry = (request.username, set(request.event_types), q)
        with self._subs_lock:
            self._subscribers.append(entry)
        context.add_callback(lambda: q.put(_STOP))
        return self._event_stream(entry)

    def _event_stream(self, entry):
        _, event_types, q = entry
        try:
            while True:
                event = q.get()
                if event is _STOP:
                    return
                if event_types and event.event_type not in event_types:
                    continue
                yield event
        finally:
            with self._subs_lock:
                if entry in self._subscribers:
                    self._subscribers.remove(entry)

    # ---------- TODO(9) ----------
    def SearchKeywords(self, request_iterator, context):
        total = 0
        hits = []
        for entry in request_iterator:          # un flux ne se rembobine pas
            if not entry.keyword:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "empty keyword")
            total += 1
            needle = entry.keyword.lower()
            with self._lock:
                count = sum(
                    1 for t in self._tasks.values()
                    if needle in t["title"].lower()
                    or needle in t["description"].lower())
            hits.append(taskflow_pb2.SearchSummary.KeywordHit(
                keyword=entry.keyword, match_count=count))
        return taskflow_pb2.SearchSummary(total_requests=total, results=hits)

    # ----- Utilitaires fournis -----
    def _get_or_abort(self, task_id: str, context) -> dict:
        """Renvoie la tâche stockée ou abort NOT_FOUND (à appeler sous le verrou)."""
        t = self._tasks.get(task_id)
        if t is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"task {task_id} not found")
        return t

    def _to_pb(self, t: dict) -> taskflow_pb2.Task:
        comments = [taskflow_pb2.Comment(author=c["author"], text=c["text"],
                                         created_at=c["created_at"])
                    for c in t.get("comments", [])]
        return taskflow_pb2.Task(
            id=t["id"], title=t["title"], description=t["description"],
            status=t["status"], assigned_to=t["assigned_to"],
            created_by=t["created_by"], created_at=t["created_at"],
            comments=comments)

    def _publish(self, event_type: str, task_id: str, author: str, message: str):
        """Envoie un événement à tous les abonnés (fourni)."""
        event = taskflow_pb2.TaskEvent(event_type=event_type, task_id=task_id,
                                       author=author, message=message)
        with self._subs_lock:
            for _, _, q in self._subscribers:
                q.put(event)


# bonus B5 : tokens de démo (utilisateur -> token). En vrai ils viendraient
# d'une base ou d'un fichier de secrets, pas du code.
DEMO_TOKENS = {
    "alice": "tok-alice",
    "bob": "tok-bob",
    "carol": "tok-carol",
}


def serve():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--slow", action="store_true", help="bonus B1")
    parser.add_argument("--auth", action="store_true",
                        help="bonus B5 : exige le metadata x-token")
    args = parser.parse_args()

    # Logging en premier : il voit aussi les appels refusés par AuthInterceptor
    interceptors = [LoggingInterceptor()]
    if args.auth:
        interceptors.append(AuthInterceptor(DEMO_TOKENS))

    # Chaque RPC en cours occupe un thread du pool ; un abonné Subscribe
    # en occupe un EN PERMANENCE -> prévoir large.
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32),
                         interceptors=interceptors)
    taskflow_pb2_grpc.add_TaskFlowServicer_to_server(TaskFlowService(args.slow), server)
    server.add_insecure_port(f"[::]:{args.port}")
    server.start()
    print(f"TaskFlow server listening on :{args.port}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=1)


if __name__ == "__main__":
    serve()
