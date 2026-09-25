# TaskFlow (gRPC)

Serveur + client gRPC de gestion de tâches (unary, server streaming avec
`ListTasks` et `Subscribe`, client streaming avec `SearchKeywords`), avec
intercepteurs de logs (serveur) et de metadata `x-user` (client).

## Mise en route

```bash
uv sync                      # ou: pip install -r requirements.txt
python -m grpc_tools.protoc -Iprotos --python_out=. --grpc_python_out=. protos/taskflow.proto
python server.py --port 50051
python client.py --user alice            # dans un autre terminal
python test_flow.py                      # tests de bout en bout (port 50061)
```

## Binôme et répartition

| Rôle | Membre | Séance 1 | Séance 2 |
|------|--------|----------|----------|
| A | MA (matioku) | `server.py` TODO(1) à (9) + bonus B1 `--slow` | `HeaderInterceptor` TODO(23) + test TODO(25) |
| B | TM (Thibaud Mineau) | `client.py` TODO(10) à (18), `test_flow.py` TODO(19) à (21) + (24) | `LoggingInterceptor` TODO(22) + branchement dans `server.py` |

Le branchement du `HeaderInterceptor` dans `client.py` a été fait par TM
(c'est son fichier), celui de `test_flow.py` par MA avec le TODO(25).
Bonus : B1 par MA, B5 par TM (qui a ajouté pour ça un paramètre `token`
optionnel au `HeaderInterceptor` de MA).

## Validation

| TODO | Fichier | Auteur | Validateur |
|------|---------|--------|------------|
| 1 à 9 | `server.py` | MA | TM |
| 10 à 18 | `client.py` | TM | MA |
| 19, 20, 21, 24 | `test_flow.py` | TM | MA |
| 22 | `interceptors.py` (`LoggingInterceptor`) | TM | MA |
| 23 | `interceptors.py` (`HeaderInterceptor`) | MA | TM |
| 25 | `test_flow.py` | MA | TM |
| B1 | `server.py --slow` / `client.py --timeout` | MA | TM |
| B5 | `AuthInterceptor`, `server.py --auth`, `client.py --token` | TM | en attente (MA) |

### Code de B validé par MA — 2026-09-25 (revue + exécution)

- `test_flow.py` : `✅ Tous les tests passent.` (TODO 19, 20, 21, 24, 25).
  TODO(25) contrôlé en négatif : sans `intercept_channel`, l'assertion
  `user=testeur` échoue bien.
- `client.py` : parcours interactif complet contre un vrai serveur (création,
  liste, détail, statut, réassignation, commentaire, recherche streaming,
  suppression, erreur `NOT_FOUND`, événements reçus) — les 5 événements
  attendus arrivent sur le flux `Subscribe`.
- `LoggingInterceptor` : une ligne par RPC, tous types confondus, au format
  `[HH:MM:SS] METHOD duration=XXms code=XX user=YY` ; `user` vient du metadata
  `x-user` (`-` si le channel n'est pas intercepté, cas du `Subscribe` du
  TODO 24), code correct sur erreur (`NOT_FOUND`) et en fin de flux
  (`CANCELLED`), durée mesurée jusqu'à l'épuisement du flux.

### Code de A validé par TM — 2026-09-25 (revue + exécution)

- `server.py` : relu TODO par TODO. Ordre des vérifications de `UpdateStatus`
  conforme (NOT_FOUND, même statut, DONE), verrou toujours pris avec `with`,
  événements publiés hors du verrou, `ListTasks` copie sous le verrou et
  `yield` en dehors, `Subscribe` bien découpé en `Subscribe` + `_event_stream`.
- Remarque (pas corrigée, cas très rare) : `context.add_callback` renvoie
  `False` si le RPC est déjà terminé. Dans ce cas `_STOP` n'est jamais posé et
  le thread reste bloqué dans `q.get()`.
- `HeaderInterceptor` : `x-user` arrive sur tous les types d'appel du service
  (unary, server streaming, client streaming), vérifié dans le log serveur
  (`CreateTask`, `ListTasks`, `Subscribe`, `SearchKeywords` avec `user=alice`).
- TODO(25) : passe, et casse bien si on retire `intercept_channel`.
- B1 : testé (voir plus bas).

## Démonstration (étape 6)

Lancé avec `alice`, `bob --events CREATED,DELETED` et `carol --no-listen`.

| # | Action | Observé |
|---|--------|---------|
| 1 | alice crée « Rédiger le rapport » | id affiché, bob reçoit 🔔 CREATED, carol rien |
| 2 | alice crée une tâche au titre vide | `❌ Erreur gRPC [INVALID_ARGUMENT] : title is required` |
| 3 | alice réassigne à bob | bob ne reçoit rien (ASSIGNED filtré), carol rien |
| 4 | bob liste les tâches | la tâche apparaît en `[TODO]` |
| 5 | bob passe à IN_PROGRESS puis DONE | alice reçoit 2 × 🔔 STATUS_CHANGED |
| 6 | bob repasse à DONE | `INVALID_ARGUMENT : task already in status DONE` |
| 7 | alice repasse à IN_PROGRESS | `INVALID_ARGUMENT : cannot reopen a DONE task` |
| 8 | carol commente | alice reçoit 🔔 COMMENTED |
| 9 | alice « Voir une tâche » | tâche + commentaire de carol avec la date |
| 10 | bob supprime la tâche d'alice | `PERMISSION_DENIED : only alice can delete this task` |
| 11 | alice cherche « rapport », « beta » | rapport → 1, beta → 0 |
| 12 | bob fait Ctrl+C | log serveur : `Subscribe duration=14134ms code=CANCELLED user=bob` tout de suite ; alice recrée une tâche sans problème |
| 13 | on coupe le serveur | alice : `⚠️ flux coupé [UNAVAILABLE]`, pas de traceback ; carol : `❌ Erreur gRPC [UNAVAILABLE]` |

Petit ajout côté client : on peut taper seulement les 8 premiers caractères de
l'id (ceux affichés par la liste). Le client retrouve l'id complet avec un
`ListTasks`, donc on voit une ligne `ListTasks` en plus dans le log serveur
avant chaque action sur une tâche.

## Question 1

**(a)** En proto3 la première valeur d'un enum doit valoir 0, et c'est aussi la
valeur par défaut. On met `TODO = 0` parce que c'est l'état normal d'une tâche
qui vient d'être créée. Le problème c'est qu'une valeur par défaut n'est pas
envoyée sur le réseau : pour `new_status` (pas `optional`), on ne peut pas faire
la différence entre « on demande TODO » et « le champ n'a pas été rempli », et
`HasField("new_status")` lève une erreur. `status_filter` est marqué `optional`,
donc protoc garde une info de présence : `HasField("status_filter")` marche et
on peut filtrer sur TODO. Sans `optional`, filtrer sur TODO reviendrait à ne pas
filtrer du tout (c'est testé dans `test_flow.py`).

**(b)** Un champ scalaire contient une seule valeur. `repeated Comment comments`
est une liste de 0 à n commentaires, dans l'ordre, vide par défaut. En Python on
la manipule comme une liste (`len`, boucle `for`, `add`/`append`), et il n'y a
pas de `HasField` dessus : une liste vide = pas de commentaire.

**(c)** `KeywordHit` ne sert que dans la réponse de `SearchKeywords`. En
l'imbriquant, on le range sous `SearchSummary.KeywordHit` : on comprend tout de
suite à quoi il sert en lisant le proto et on ne pollue pas le package avec un
nom qui pourrait entrer en conflit plus tard. On peut quand même l'utiliser
ailleurs avec son nom complet si besoin.

## Question piège (12)

Si la queue de bob n'était pas retirée à la déconnexion, `_publish`
continuerait de lui envoyer chaque événement alors que plus personne ne la lit :
la queue grossit sans fin, c'est une fuite mémoire. La liste `_subscribers`
grossit aussi à chaque déconnexion/reconnexion et chaque `_publish` devient plus
lent. Et si en plus le thread restait bloqué dans `q.get()`, chaque abonné
fantôme garderait un thread du pool : au bout de 32 (`max_workers`) le serveur
ne peut plus traiter aucun RPC et tout part en `DEADLINE_EXCEEDED`. Chez nous le
callback `add_callback` pose `_STOP`, `_event_stream` sort et le `finally`
retire l'abonné, c'est ce qu'on voit avec la ligne `code=CANCELLED` au Ctrl+C.

## Bonus B1 — Deadline

```bash
python server.py --slow
python client.py --user alice --timeout 1
```

Le client affiche `❌ Erreur gRPC [DEADLINE_EXCEEDED] : Deadline Exceeded` au
bout d'1 s. Le log serveur :

```
[16:37:52] /taskflow.TaskFlow/CreateTask  duration=5006ms  code=DEADLINE_EXCEEDED  user=alice
```

Et pourtant la tâche existe : bob la voit dans la liste juste après. La
deadline fait abandonner le client et le RPC est annulé côté serveur, mais gRPC
ne peut pas interrompre un thread Python qui est dans `time.sleep(5)`. Le
handler continue, enregistre la tâche et publie `CREATED`, c'est juste la
réponse qui est jetée. Pour l'éviter il faudrait vérifier `context.is_active()`
avant d'écrire, ou rendre la création idempotente pour que le client puisse
réessayer sans créer de doublon.

## Bonus B5 — Authentification

```bash
python server.py --auth
python client.py --user alice --token tok-alice
```

- `AuthInterceptor` (serveur, dans `interceptors.py`) lit `x-token` dans le
  metadata. Pas de token → `UNAUTHENTICATED`. Token qui n'est pas celui de
  `x-user` (token faux, ou token d'un autre utilisateur) → `PERMISSION_DENIED`.
  On ne peut donc pas se faire passer pour quelqu'un d'autre en changeant juste
  `--user`.
- Tokens de démo dans `server.py` (`DEMO_TOKENS`) : `alice`/`tok-alice`,
  `bob`/`tok-bob`, `carol`/`tok-carol`.
- Côté client, `HeaderInterceptor(user, token)` ajoute aussi `x-token` quand un
  token est donné (option `--token`).
- Dans `intercept_service` on n'a pas encore de `context`, donc on ne peut pas
  faire `abort` directement : on renvoie un handler du même type que l'original
  (unary ou stream) dont la fonction fait juste `context.abort(...)`.
- `LoggingInterceptor` est avant `AuthInterceptor` dans la liste, donc les refus
  apparaissent aussi dans le log :

```
[16:43:07] /taskflow.TaskFlow/CreateTask  duration=0ms  code=UNAUTHENTICATED  user=alice
[16:43:07] /taskflow.TaskFlow/CreateTask  duration=0ms  code=PERMISSION_DENIED  user=bob
[16:43:07] /taskflow.TaskFlow/Subscribe  duration=0ms  code=UNAUTHENTICATED  user=bob
```

- Testé sur les 3 types de RPC (unary, server streaming, client streaming) et
  sur `Subscribe`. Sans `--auth` rien ne change (la démo et `test_flow.py`
  marchent pareil).
- Limite : les tokens sont en clair dans le code et le channel n'est pas
  chiffré (`insecure_channel`), donc quelqu'un qui écoute le réseau peut voler
  un token. En vrai il faudrait TLS et des tokens stockés ailleurs (ou des JWT
  signés).

## Questions de compréhension

**1. Unaire vs streaming**
- Unary : `GetTask`, `CreateTask`… une requête, une réponse, c'est le cas
  classique question/réponse.
- Server streaming : `ListTasks`, parce qu'on ne sait pas combien il y a de
  tâches ; le client peut les afficher au fur et à mesure sans que le serveur
  construise un énorme message. `Subscribe` aussi : le serveur pousse les
  événements quand ils arrivent, le flux ne se termine jamais, pas besoin de
  faire du polling.
- Client streaming : `SearchKeywords`, le client envoie une rafale de mots-clés
  et le serveur répond une seule fois avec une synthèse (total + résultats).

**2. Concurrence** — Le serveur utilise un `ThreadPoolExecutor(32)`, donc
chaque RPC tourne dans son propre thread et ils touchent tous au même dict
`_tasks`. Exemple : bob passe une tâche à DONE pendant qu'alice la passe à
IN_PROGRESS. Sans verrou, les deux lisent « pas DONE », les deux écrivent, et on
peut se retrouver avec une tâche DONE « réouverte » alors que c'est interdit.
Autre cas : `SearchKeywords` parcourt `_tasks` pendant qu'un `CreateTask`
ajoute une tâche → `RuntimeError: dictionary changed size during iteration`.
Un `RLock` peut être repris par le thread qui le tient déjà : si une méthode qui
a le verrou appelle une autre méthode qui le prend aussi, un `Lock` normal
bloquerait le thread sur lui-même.

**3. REST vs gRPC pour `Subscribe`** — En REST on aurait trois options :
polling (appeler `GET /events` toutes les X secondes), SSE (flux HTTP du serveur
vers le client) ou WebSocket (canal bidirectionnel).
- Réactivité / coût : le polling a une latence égale à la période et fait
  plein de requêtes inutiles ; SSE, WebSocket et gRPC poussent tout de suite.
- Contrat : avec gRPC les messages sont typés par le `.proto` et le code est
  généré ; en SSE/WebSocket c'est du texte/JSON libre qu'il faut documenter et
  valider soi-même.
- Navigateur : SSE et WebSocket marchent directement dans un navigateur, gRPC
  demande gRPC-Web + un proxy. Par contre gRPC profite de HTTP/2 (plusieurs flux
  sur une seule connexion) et du binaire.

**4. `abort` au milieu d'un stream** — Les 2 tâches déjà envoyées sont parties
comme des messages normaux, le client les reçoit. Le statut (code + message)
n'est envoyé qu'à la fin du flux, dans les trailers HTTP/2 (`grpc-status`,
`grpc-message`). En Python, `for t in stub.ListTasks(...)` donne les 2 tâches
puis l'itération suivante lève `grpc.RpcError` avec le code de l'abort. Donc le
client les voit s'il les traite dans la boucle, mais avec
`list(stub.ListTasks(...))` l'exception fait perdre ce qui avait été reçu.

**5. Modèle interne vs message** — gRPC sérialise la réponse après le `return`
du handler, donc hors du verrou. Si on renvoyait l'objet stocké, un autre thread
(ex. `AddComment`) pourrait le modifier pendant la sérialisation et on
enverrait un message incohérent. En stockant des dict et en reconstruisant un
message neuf avec `_to_pb`, chaque réponse est une copie figée à un instant
donné. `AddComment` ajoute le dict du commentaire et appelle `_to_pb(task)`
**sous le verrou**, puis publie l'événement après en être sorti.

**6. Désaccord dans le binôme** — Le branchement du `HeaderInterceptor` dans
`client.py` : dans le sujet c'est l'étape 5 de A, mais `client.py` est le fichier
de B. Comme A avait déjà fini l'intercepteur et que c'était une ligne, B l'a
branché directement dans son client et A a fait celui de `test_flow.py` avec le
TODO(25). On a aussi discuté de ce que le log doit afficher quand il n'y a pas
de metadata `x-user` : on a gardé `user=-` plutôt que `user=None`, c'est plus
lisible et on voit tout de suite quel channel n'est pas intercepté (le
`Subscribe` du TODO 24).
