import socket


def recv_all(sock: socket.socket, size: int) -> bytes:
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ConnectionError("recv_all: connection closed before full message")
        data += chunk
    return data


def send_all(sock: socket.socket, data: bytes) -> None:
    total = 0
    while total < len(data):
        sent = sock.send(data[total:])
        if sent == 0:
            raise ConnectionError("send_all: sent 0 bytes")
        total += sent
