# interceptors.py
import collections
import threading
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
_print_lock = threading.Lock()  # sinon deux RPC en parallèle mélangent leurs lignes


def _final_code(context, failed):
    code = context.code()
    if code is not None:          # abort() a fixé le code
        return code.name
    if failed:                    # exception Python qui n'est pas un abort
        return "UNKNOWN"
    if not context.is_active():   # le client est déjà parti
        remaining = context.time_remaining()
        if remaining is not None and remaining <= 0:
            return "DEADLINE_EXCEEDED"
        return "CANCELLED"
    return "OK"


class LoggingInterceptor(grpc.ServerInterceptor):
    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        if handler is None:
            return None

        method = handler_call_details.method
        metadata = dict(handler_call_details.invocation_metadata or ())
        user = metadata.get("x-user", "-")

        def log(context, start, failed):
            ms = (time.perf_counter() - start) * 1000
            line = (f"[{datetime.now():%H:%M:%S}] {method}  duration={ms:.0f}ms  "
                    f"code={_final_code(context, failed)}  user={user}")
            with _print_lock:
                print(line, flush=True)

        # réponse unique (unary_unary, stream_unary) : on chronomètre l'appel
        def wrap_unary_response(behavior):
            def wrapper(request_or_iterator, context):
                start = time.perf_counter()
                failed = False
                try:
                    return behavior(request_or_iterator, context)
                except Exception:
                    failed = True
                    raise
                finally:
                    log(context, start, failed)
            return wrapper

        # réponse en flux (unary_stream, stream_stream) : le RPC n'est fini
        # qu'une fois le flux consommé, donc wrapper générateur
        def wrap_stream_response(behavior):
            def wrapper(request_or_iterator, context):
                start = time.perf_counter()
                failed = False
                try:
                    yield from behavior(request_or_iterator, context)
                except Exception:
                    failed = True
                    raise
                finally:
                    log(context, start, failed)
            return wrapper

        des, ser = handler.request_deserializer, handler.response_serializer
        if handler.unary_unary:
            return grpc.unary_unary_rpc_method_handler(
                wrap_unary_response(handler.unary_unary), des, ser)
        if handler.stream_unary:
            return grpc.stream_unary_rpc_method_handler(
                wrap_unary_response(handler.stream_unary), des, ser)
        if handler.unary_stream:
            return grpc.unary_stream_rpc_method_handler(
                wrap_stream_response(handler.unary_stream), des, ser)
        if handler.stream_stream:
            return grpc.stream_stream_rpc_method_handler(
                wrap_stream_response(handler.stream_stream), des, ser)
        return handler


# ---------- Bonus B5 ---------- (membre B)
# AuthInterceptor (serveur) : vérifie le metadata x-token avant le RPC.
#   pas de token -> UNAUTHENTICATED
#   token qui n'est pas celui de x-user -> PERMISSION_DENIED
# Pour refuser, on renvoie un handler du MÊME type que l'original (sinon gRPC
# se trompe sur le format des messages) dont la fonction fait juste abort.
def _refusing_handler(handler, code, details):
    def refuse(request_or_iterator, context):
        context.abort(code, details)

    des, ser = handler.request_deserializer, handler.response_serializer
    if handler.unary_unary:
        return grpc.unary_unary_rpc_method_handler(refuse, des, ser)
    if handler.stream_unary:
        return grpc.stream_unary_rpc_method_handler(refuse, des, ser)
    if handler.unary_stream:
        return grpc.unary_stream_rpc_method_handler(refuse, des, ser)
    return grpc.stream_stream_rpc_method_handler(refuse, des, ser)


class AuthInterceptor(grpc.ServerInterceptor):
    def __init__(self, tokens: dict):
        self._tokens = tokens  # utilisateur -> token

    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        if handler is None:
            return None

        metadata = dict(handler_call_details.invocation_metadata or ())
        user = metadata.get("x-user")
        token = metadata.get("x-token")
        if not token:
            return _refusing_handler(handler, grpc.StatusCode.UNAUTHENTICATED,
                                     "missing x-token")
        if user is None or self._tokens.get(user) != token:
            return _refusing_handler(handler, grpc.StatusCode.PERMISSION_DENIED,
                                     f"invalid token for {user}")
        return handler


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
    def __init__(self, user: str, token: str | None = None):
        self._user = user
        self._token = token  # bonus B5

    def _with_user(self, details: grpc.ClientCallDetails) -> _ClientCallDetails:
        """Recopie les détails d'appel en y ajoutant le metadata x-user
        (et x-token s'il y en a un, bonus B5)."""
        metadata = list(details.metadata or [])
        metadata.append(("x-user", self._user))
        if self._token:
            metadata.append(("x-token", self._token))
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
