import signal
import socket
import threading
import time

import logger
import protocol
import safe_socket
from coordinator.coordinator import Coordinator

_ACCEPT_POLL_TIMEOUT_SECONDS = 1.0
_SHUTDOWN_GRACE_SECONDS = 3.0


class Server:
    def __init__(
        self, server_host: str, server_port: int, agency_quorum_min: int
    ) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.coordinator = Coordinator(agency_quorum_min)
        self._shutdown = threading.Event()
        self._clients_lock = threading.Lock()
        self._client_threads: set[threading.Thread] = set()
        self._client_sockets: set[socket.socket] = set()

    def _handle_sigterm(self, signum, frame):
        self._shutdown.set()

    def _handle_client(self, client_socket, coordinator):
        action = "handle-client"
        agency_ids = set()
        bets_count = 0
        try:
            logger.info(action, logger.LogResult.in_progress)
            with client_socket:
                while True:
                    try:
                        header = safe_socket.recv_all(
                            client_socket, protocol.LENGTH_FIELD_SIZE
                        )
                    except ConnectionError:
                        break  # el cliente cerro la conexion => no hay mas apuestas

                    payload_length = protocol.decode_length(header)
                    payload = safe_socket.recv_all(client_socket, payload_length)
                    bets = protocol.decode_batch(payload)
                    bets_count += len(bets)
                    agency_ids.update(bet.agency_id for bet in bets)
                    coordinator.store_bets(bets)

                    safe_socket.send_all(client_socket, protocol.ACK)

                coordinator.register_agency(agency_ids)
                for winner in coordinator.winners_for(agency_ids):
                    safe_socket.send_all(
                        client_socket, protocol.encode_bet_message(winner)
                    )

            logger.info(action, logger.LogResult.success, "bets-amount", bets_count)
        except Exception:
            logger.error(action, logger.LogResult.fail)
            raise
        finally:
            with self._clients_lock:
                self._client_threads.discard(threading.current_thread())
                self._client_sockets.discard(client_socket)

    def _shutdown_handlers(self) -> None:
        # se espera un tiempo de gracia a que terminen solos
        deadline = time.time() + _SHUTDOWN_GRACE_SECONDS
        while time.time() < deadline:
            with self._clients_lock:
                threads = list(self._client_threads)
            if not threads:
                return
            for thread in threads:
                thread.join(timeout=0.1)

        # se fuerzan los que quedaron
        with self._clients_lock:
            surviving_threads = list(self._client_threads)
            surviving_sockets = list(self._client_sockets)
        for client_socket in surviving_sockets:
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass  # ya estaba cerrado
        for thread in surviving_threads:
            thread.join()

    def run(self):
        action = "accept-connection"
        coordinator = self.coordinator

        signal.signal(signal.SIGTERM, self._handle_sigterm)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()

            # timeout en accept: cada 1s revise el flag de SIGTERM aunque no lleguen conexiones
            server_socket.settimeout(_ACCEPT_POLL_TIMEOUT_SECONDS)

            while not self._shutdown.is_set():
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except TimeoutError:
                    continue
                except Exception:
                    logger.error(action, logger.LogResult.fail)
                    raise
                logger.info(action, logger.LogResult.success)

                thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, coordinator),
                )
                with self._clients_lock:
                    self._client_sockets.add(client_socket)
                    self._client_threads.add(thread)

                thread.start()

        coordinator.shutdown()
        self._shutdown_handlers()
