import socket
import tempfile

import logger
import safe_socket
import protocol
from lottery.lottery import Lottery


class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port

    def _handle_client(self, client_socket):
        action = "handle-client"
        try:
            logger.info(action, logger.LogResult.in_progress)

            with tempfile.NamedTemporaryFile() as storage:
                lottery = Lottery(storage.name)

                bets_count = 0
                while True:
                    try:
                        header = safe_socket.recv_all(
                            client_socket, protocol.LENGTH_FIELD_SIZE
                        )
                    except ConnectionError:
                        break  # el cliente cerró la conexión = no hay más apuestas

                    payload_length = protocol.decode_length(header)
                    payload = safe_socket.recv_all(client_socket, payload_length)
                    bet = protocol.decode_bet(payload)
                    bets_count += 1
                    lottery.store_bets([bet])

                winners = [
                    bet for bet in lottery.load_bets() if lottery.has_won(bet)
                ]
                for winner in winners:
                    safe_socket.send_all(client_socket, protocol.encode_wire(winner))

            logger.info(action, logger.LogResult.success, "bets-amount", bets_count)
            client_socket.close()
        except Exception as e:
            logger.error(action, logger.LogResult.fail)
            raise e

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                self._handle_client(client_socket)