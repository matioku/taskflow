# test_flow.py — python test_flow.py
import queue
import subprocess
import sys
import tempfile
import threading
import time

import grpc

import taskflow_pb2 as pb
import taskflow_pb2_grpc

PORT = 50061  # port dédié aux tests : ne gêne pas un serveur de démo déjà lancé
T = 3         # timeout des appels


def expect_error(fn, code):
    """Appelle fn() et vérifie qu'elle échoue avec le code gRPC attendu."""
    try:
        fn()
    except grpc.RpcError as e:
        assert e.code() == code, f"attendu {code}, reçu {e.code()}"
        return
    raise AssertionError(f"attendu une erreur {code}, aucun échec")


def entries(keywords):
    for k in keywords:
        yield pb.SearchEntry(keyword=k)


def run_tests(stub, log_path):
    # --- TODO(19) : scénario nominal ---
    resp = stub.CreateTask(pb.CreateTaskRequest(
        title="Rapport", assigned_to="alice", created_by="alice"), timeout=T)
    tid = resp.task.id
    assert tid, "CreateTask n'a pas renvoyé d'id"

    task = stub.GetTask(pb.GetTaskRequest(id=tid), timeout=T)
    assert task.title == "Rapport", task.title
    assert task.status == pb.TODO, task.status

    task = stub.UpdateStatus(pb.UpdateStatusRequest(
        id=tid, new_status=pb.DONE, requested_by="alice"), timeout=T)
    assert task.status == pb.DONE, task.status

    tasks = list(stub.ListTasks(pb.ListTasksRequest(), timeout=T))
    assert len(tasks) >= 1, "ListTasks ne renvoie rien"
    done = list(stub.ListTasks(pb.ListTasksRequest(status_filter=pb.DONE), timeout=T))
    assert [t.id for t in done] == [tid], done
    # filtre à TODO (= 0) : doit quand même filtrer grâce à optional
    todo = list(stub.ListTasks(pb.ListTasksRequest(status_filter=pb.TODO), timeout=T))
    assert todo == [], todo
    print("TODO(19) ok")

    # --- TODO(20) : erreurs attendues ---
    expect_error(lambda: stub.GetTask(pb.GetTaskRequest(
        id="inconnu"), timeout=T), grpc.StatusCode.NOT_FOUND)
    expect_error(lambda: stub.CreateTask(pb.CreateTaskRequest(
        title="", created_by="alice"), timeout=T),
        grpc.StatusCode.INVALID_ARGUMENT)
    expect_error(lambda: stub.UpdateStatus(pb.UpdateStatusRequest(
        id=tid, new_status=pb.DONE, requested_by="alice"), timeout=T),
        grpc.StatusCode.INVALID_ARGUMENT)
    expect_error(lambda: stub.UpdateStatus(pb.UpdateStatusRequest(
        id=tid, new_status=pb.IN_PROGRESS, requested_by="alice"), timeout=T),
        grpc.StatusCode.INVALID_ARGUMENT)
    expect_error(lambda: stub.DeleteTask(pb.DeleteTaskRequest(
        id=tid, requested_by="bob"), timeout=T),
        grpc.StatusCode.PERMISSION_DENIED)
    stub.DeleteTask(pb.DeleteTaskRequest(id=tid, requested_by="alice"), timeout=T)
    expect_error(lambda: stub.GetTask(pb.GetTaskRequest(id=tid), timeout=T),
                 grpc.StatusCode.NOT_FOUND)
    print("TODO(20) ok")

    # --- TODO(21) : client streaming ---
    for title in ("alpha", "beta", "alpha beta"):
        stub.CreateTask(pb.CreateTaskRequest(title=title, created_by="alice"),
                        timeout=T)
    summary = stub.SearchKeywords(entries(["alpha", "BETA"]), timeout=T)
    assert summary.total_requests == 2, summary.total_requests
    hits = {h.keyword: h.match_count for h in summary.results}
    assert hits == {"alpha": 2, "BETA": 2}, hits
    expect_error(lambda: stub.SearchKeywords(entries(["alpha", "", "beta"]),
                                             timeout=T),
                 grpc.StatusCode.INVALID_ARGUMENT)
    print("TODO(21) ok")

    # --- TODO(24) : Subscribe ---
    sub_channel = grpc.insecure_channel(f"localhost:{PORT}")
    sub_stub = taskflow_pb2_grpc.TaskFlowStub(sub_channel)
    events = queue.Queue()

    def listen():
        try:
            for event in sub_stub.Subscribe(pb.SubscribeRequest(
                    username="observateur", event_types=["DELETED"])):
                events.put(event)
        except grpc.RpcError:
            pass  # CANCELLED quand on ferme sub_channel

    threading.Thread(target=listen, daemon=True).start()
    try:
        time.sleep(0.3)  # le temps que l'abonnement soit pris en compte
        t = stub.CreateTask(pb.CreateTaskRequest(
            title="temporaire", created_by="alice"), timeout=T).task
        stub.DeleteTask(pb.DeleteTaskRequest(id=t.id, requested_by="alice"),
                        timeout=T)
        try:
            event = events.get(timeout=2)
        except queue.Empty:
            raise AssertionError("aucun événement reçu par l'abonné")
        # le CREATED est arrivé avant : s'il n'avait pas été filtré on l'aurait ici
        assert event.event_type == "DELETED", event.event_type
        assert event.task_id == t.id
    finally:
        sub_channel.close()
    print("TODO(24) ok")

    # --- TODO(25) : Étape 5 — metadata x-user ---
    # Le stdout du serveur est écrit dans log_path. Vérifiez qu'il contient
    # "user=testeur" (metadata ajouté par HeaderInterceptor) et une ligne
    # avec code=NOT_FOUND (produite par LoggingInterceptor).
    pass


def main():
    log = tempfile.NamedTemporaryFile("w+", suffix=".log", delete=False)
    proc = subprocess.Popen([sys.executable, "server.py", "--port", str(PORT)],
                            stdout=log, stderr=subprocess.STDOUT)
    channel = grpc.insecure_channel(f"localhost:{PORT}")
    try:
        grpc.channel_ready_future(channel).result(timeout=10)  # attend le serveur
        # Étape 5 : channel = grpc.intercept_channel(channel, HeaderInterceptor("testeur"))
        run_tests(taskflow_pb2_grpc.TaskFlowStub(channel), log.name)
        print("✅ Tous les tests passent.")
    finally:
        channel.close()
        proc.terminate()   # toujours exécuté, même si un assert échoue
        proc.wait()


if __name__ == "__main__":
    main()
