# TaskFlow (gRPC)

Serveur + client gRPC de gestion de tâches (unary, server streaming, client
streaming, bidi-ish via `Subscribe`), avec intercepteurs de logs (serveur) et
de metadata `x-user` (client).

## Mise en route

```bash
uv sync                      # ou: pip install -r requirements.txt
python -m grpc_tools.protoc -Iprotos --python_out=. --grpc_python_out=. protos/taskflow.proto
python server.py --port 50051
python client.py --user alice            # dans un autre terminal
python test_flow.py                      # tests de bout en bout (port 50061)
```

## Validation

Validé par : **CC** — 2026-09-25 (revue + exécution ; renommer si les initiales
du binôme sont attendues).

Périmètre vérifié :

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
