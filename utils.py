import socket

def create_server_socket(port):
    """
    Creates a socket for a server.

    Args:
        port (int): Port which the server listens to.

    Returns:
        socket.socket: socket on the server side.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", port))
    s.listen()
    return s

def create_client_socket():
    """
    Creates a socket for a client.

    Returns:
        socket.socket: socket on the client side.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(4000) #non-blocking mode
    return s
