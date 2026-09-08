import socket
import threading

import logger
import safe_socket
import protocol
from coordinator.coordinator import Coordinator


class Server:
    def __init__(self, server_host: str, server_port: int, agency_quorum_min: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.coordinator = Coordinator(agency_quorum_min)

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
                        break  # el cliente cerró la conexión = no hay más apuestas

                    payload_length = protocol.decode_length(header)
                    payload = safe_socket.recv_all(client_socket, payload_length)
                    bets = protocol.decode_batch(payload)
                    bets_count += len(bets)
                    agency_ids.update(bet.agency_id for bet in bets)
                    coordinator.store_bets(bets)

                coordinator.register_agency()
                for winner in coordinator.winners_for(agency_ids):
                    safe_socket.send_all(client_socket, protocol.encode_wire(winner))

            logger.info(action, logger.LogResult.success, "bets-amount", bets_count)
        except Exception:
            logger.error(action, logger.LogResult.fail)
            raise

    def run(self):
        action = "accept-connection"
        coordinator = self.coordinator
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception:
                    logger.error(action, logger.LogResult.fail)
                    raise
                logger.info(action, logger.LogResult.success)

                thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, coordinator),
                    daemon=True,
                )
                thread.start()