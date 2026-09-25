# interceptors.py
import collections
import time
from datetime import datetime

import grpc


# ---------- TODO(22) ---------- (membre B)
# LoggingInterceptor (serveur) : à chaque RPC, afficher
#   [HH:MM:SS] METHOD  duration=XXms  code=XX  user=YY
#
# ⚠️ grpc.ServerInterceptor n'a qu'UNE méthode : intercept_service().
#    Elle est appelée AVANT le RPC : continuation(handler_call_details)
#    renvoie un RpcMethodHandler, sans exécuter le RPC. Chronométrer
#    autour de continuation() donnerait donc toujours ~0 ms.
#
# Démarche :
#   1. handler = continuation(handler_call_details) (None -> return None)
#   2. selon le type (handler.unary_unary, .unary_stream, .stream_unary,
#      .stream_stream), envelopper la fonction dans un wrapper qui
#      chronomètre avec time.perf_counter() :
#        - réponse unique  : autour de l'appel à la fonction
#        - réponse en flux : wrapper GÉNÉRATEUR (yield from ...), log à la fin
#   3. reconstruire le handler avec grpc.unary_unary_rpc_method_handler(
#        wrapper, handler.request_deserializer, handler.response_serializer)
#      (idem unary_stream_…, stream_unary_…, stream_stream_…)
#   4. code : context.code() (None = OK ; exception non-abort = UNKNOWN)
#   5. user : dict(handler_call_details.invocation_metadata).get("x-user")
#   Pensez à print(..., flush=True).
class LoggingInterceptor(grpc.ServerInterceptor):
    def intercept_service(self, continuation, handler_call_details):
        raise NotImplementedError()


# ---------- TODO(23) ---------- (membre A)
class _ClientCallDetails(
        collections.namedtuple(
            "_ClientCallDetails",
            ("method", "timeout", "metadata", "credentials",
             "wait_for_ready", "compression")),
        grpc.ClientCallDetails):
    pass


class HeaderInterceptor(grpc.UnaryUnaryClientInterceptor,
                        grpc.UnaryStreamClientInterceptor,
                        grpc.StreamUnaryClientInterceptor,
                        grpc.StreamStreamClientInterceptor):
    def __init__(self, user: str):
        self._user = user

    def _with_user(self, details: grpc.ClientCallDetails) -> _ClientCallDetails:
        """Recopie les détails d'appel en y ajoutant le metadata x-user."""
        metadata = list(details.metadata or [])
        metadata.append(("x-user", self._user))
        return _ClientCallDetails(details.method, details.timeout, metadata,
                                  details.credentials, details.wait_for_ready,
                                  details.compression)

    def intercept_unary_unary(self, continuation, details, request):
        return continuation(self._with_user(details), request)

    def intercept_unary_stream(self, continuation, details, request):
        return continuation(self._with_user(details), request)

    def intercept_stream_unary(self, continuation, details, request_iterator):
        return continuation(self._with_user(details), request_iterator)

    def intercept_stream_stream(self, continuation, details, request_iterator):
        return continuation(self._with_user(details), request_iterator)
