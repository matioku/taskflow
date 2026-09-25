# client.py
import argparse
import threading

import grpc

import taskflow_pb2
import taskflow_pb2_grpc
from interceptors import HeaderInterceptor

STATUS_NAMES = {0: "TODO", 1: "IN_PROGRESS", 2: "DONE"}
received_events = []  # pour l'option 9 (débug)


def print_event(event):
    print(f"\n🔔 [{event.event_type}] {event.author}: {event.message}\n> ",
          end="", flush=True)


# ---------- TODO(10) ----------
def listen_events(stub, username, event_types):
    try:
        stream = stub.Subscribe(taskflow_pb2.SubscribeRequest(
            username=username, event_types=event_types))
        for event in stream:
            received_events.append(event)
            print_event(event)
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.CANCELLED:
            return  # c'est nous qui fermons le channel en quittant
        print(f"\n⚠️  flux coupé [{e.code().name}] : {e.details()}\n> ",
              end="", flush=True)


def print_task(task):
    print(f"  [{STATUS_NAMES[task.status]:12}] {task.id[:8]}… "
          f"« {task.title} » → {task.assigned_to or 'non assignée'} "
          f"({len(task.comments)} commentaire(s))")


def ask_id(stub, timeout):
    """Demande l'id d'une tâche. On accepte aussi le début de l'id (les 8
    caractères affichés par la liste) s'il ne correspond qu'à une tâche."""
    task_id = input("id de la tâche > ").strip()
    if not task_id or len(task_id) == 36:
        return task_id
    matches = [t.id for t in stub.ListTasks(taskflow_pb2.ListTasksRequest(),
                                            timeout=timeout)
               if t.id.startswith(task_id)]
    if len(matches) == 1:
        return matches[0]
    return task_id  # aucun ou plusieurs résultats : le serveur répondra NOT_FOUND


def ask_status(allow_empty=False):
    """Demande un statut (numéro ou nom). Renvoie None si vide et autorisé."""
    while True:
        s = input("statut (0=TODO, 1=IN_PROGRESS, 2=DONE"
                  + (", vide = tous" if allow_empty else "") + ") > ").strip().upper()
        if not s and allow_empty:
            return None
        if s in ("0", "1", "2"):
            return int(s)
        if s in STATUS_NAMES.values():
            return taskflow_pb2.TaskStatus.Value(s)
        print("statut invalide")


def fmt_date(ts):
    return ts.ToDatetime().isoformat(sep=" ", timespec="seconds")


def search_entries(keywords):
    for k in keywords:
        yield taskflow_pb2.SearchEntry(keyword=k)


def main():
    parser = argparse.ArgumentParser(description="Client TaskFlow")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--user", required=True)
    parser.add_argument("--events", default="",
                        help="filtre, ex: CREATED,DELETED (vide = tout)")
    parser.add_argument("--no-listen", action="store_true")
    parser.add_argument("--timeout", type=float, default=3, help="bonus B1")
    parser.add_argument("--token", help="bonus B5 (serveur lancé avec --auth)")
    args = parser.parse_args()

    channel = grpc.insecure_channel(f"{args.host}:{args.port}")
    channel = grpc.intercept_channel(channel,
                                     HeaderInterceptor(args.user, args.token))
    stub = taskflow_pb2_grpc.TaskFlowStub(channel)
    T = args.timeout  # à passer en timeout=T sur TOUS les appels (sauf Subscribe)

    event_types = [e.strip().upper() for e in args.events.split(",") if e.strip()]

    if not args.no_listen:
        threading.Thread(target=listen_events,
                         args=(stub, args.user, event_types),
                         daemon=True).start()

    while True:
        print("""
=== TaskFlow ===  (utilisateur: {u})
 1. Créer une tâche        6. Commenter une tâche
 2. Lister les tâches      7. Supprimer une tâche
 3. Voir une tâche         8. Recherche multi-mots-clés (streaming)
 4. Changer le statut      9. Événements reçus (aide au débug)
 5. Réassigner             0. Quitter""".format(u=args.user))

        try:
            choice = input("choix > ").strip()
            if choice == "1":
                # ---------- TODO(11) ----------
                title = input("titre > ").strip()
                description = input("description > ").strip()
                assignee = input("assignée à (vide = personne) > ").strip()
                resp = stub.CreateTask(taskflow_pb2.CreateTaskRequest(
                    title=title, description=description,
                    assigned_to=assignee, created_by=args.user), timeout=T)
                print(f"✅ tâche créée, id = {resp.task.id}")
            elif choice == "2":
                # ---------- TODO(12) ----------
                req = taskflow_pb2.ListTasksRequest()
                status = ask_status(allow_empty=True)
                if status is not None:
                    req.status_filter = status  # marche aussi pour TODO = 0
                assignee = input("assignée à (vide = tous) > ").strip()
                if assignee:
                    req.assigned_filter = assignee
                count = 0
                for task in stub.ListTasks(req, timeout=T):
                    print_task(task)
                    count += 1
                print(f"{count} tâche(s)")
            elif choice == "3":
                # ---------- TODO(13) ----------
                task_id = ask_id(stub, T)
                task = stub.GetTask(taskflow_pb2.GetTaskRequest(id=task_id),
                                    timeout=T)
                print_task(task)
                print(f"  id          : {task.id}")
                print(f"  description : {task.description or '-'}")
                print(f"  créée par {task.created_by} le {fmt_date(task.created_at)} (UTC)")
                for c in task.comments:
                    print(f"    💬 {c.author} ({fmt_date(c.created_at)}) : {c.text}")
            elif choice == "4":
                # ---------- TODO(14) ----------
                task_id = ask_id(stub, T)
                new_status = ask_status()
                task = stub.UpdateStatus(taskflow_pb2.UpdateStatusRequest(
                    id=task_id, new_status=new_status,
                    requested_by=args.user), timeout=T)
                print_task(task)
            elif choice == "5":
                # ---------- TODO(15) ----------
                task_id = ask_id(stub, T)
                assignee = input("nouvelle personne assignée > ").strip()
                task = stub.AssignTask(taskflow_pb2.AssignTaskRequest(
                    id=task_id, new_assignee=assignee,
                    requested_by=args.user), timeout=T)
                print_task(task)
            elif choice == "6":
                # ---------- TODO(16) ----------
                task_id = ask_id(stub, T)
                text = input("commentaire > ").strip()
                task = stub.AddComment(taskflow_pb2.AddCommentRequest(
                    id=task_id, author=args.user, text=text), timeout=T)
                print_task(task)
            elif choice == "7":
                # ---------- TODO(17) ----------
                task_id = ask_id(stub, T)
                stub.DeleteTask(taskflow_pb2.DeleteTaskRequest(
                    id=task_id, requested_by=args.user), timeout=T)
                print("🗑️  tâche supprimée")
            elif choice == "8":
                # ---------- TODO(18) ----------
                # on récupère d'abord tous les mots, sinon le timeout
                # expire pendant qu'on tape
                keywords = []
                while True:
                    k = input(f"mot-clé {len(keywords) + 1} (vide = fin) > ").strip()
                    if not k:
                        break
                    keywords.append(k)
                summary = stub.SearchKeywords(search_entries(keywords), timeout=T)
                print(f"{summary.total_requests} mot(s)-clé(s) envoyé(s)")
                for hit in summary.results:
                    print(f"  {hit.keyword} → {hit.match_count} tâche(s)")
            elif choice == "9":
                print(f"{len(received_events)} événement(s) reçu(s)")
            elif choice == "0":
                print("Au revoir !")
                break
            else:
                print("choix invalide")
        except grpc.RpcError as e:
            print(f"❌ Erreur gRPC [{e.code().name}] : {e.details()}")
        except (KeyboardInterrupt, EOFError):
            print()
            break
    channel.close()


if __name__ == "__main__":
    main()
