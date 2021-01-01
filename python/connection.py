import bitarray
import message
import random
import threading


class Connection(threading.Thread):

    def __init__(self, sock, initiate_handshake, peer):
        super(Connection, self).__init__()
        self.sock = sock
        self.initiate_handshake = initiate_handshake
        self.peer = peer

        self.other_peer_id = -1
        self.other_arr = bitarray.bitarray(peer.num_pieces)
        self.other_arr.setall(False)
        self.interested = False
        self.choked = True
        self.pieces_received = 0

    def run(self):
        try:
            if self.initiate_handshake:
                self.send(message.handshake(self.peer.peer_id))

            while self.peer.missing_pieces() or self.peer.neighbor_missing_pieces():
                self.respond(self.rcv())

            self.sock.close()
            self.peer.connections.remove(self)
            print(f'Peer {self.peer.peer_id} closed connection with Peer {self.other_peer_id}')
        except ConnectionError:
            print(f'Peer {self.peer.peer_id}-{self.other_peer_id} connection was closed')

    def respond(self, msg):
        if msg.type == message.HANDSHAKE:
            self.other_peer_id = int.from_bytes(msg.payload, 'big')
            if not self.initiate_handshake:
                self.send(message.handshake(self.peer.peer_id))
            if self.peer.has_file:
                self.send(message.bitfield(self.peer.arr))
        elif msg.type == message.CHOKE:
            pass
        elif msg.type == message.UNCHOKE:
            self._request_piece()
        elif msg.type == message.INTERESTED:
            self.interested = True
        elif msg.type == message.NOT_INTERESTED:
            self.interested = False
        elif msg.type == message.HAVE:
            index = int.from_bytes(msg.payload, 'big')
            self.other_arr[index] = True
            self._notify_interest()
        elif msg.type == message.BITFIELD:
            self.other_arr.clear()
            self.other_arr.frombytes(msg.payload)
            self._notify_interest()
        elif msg.type == message.REQUEST:
            index = int.from_bytes(msg.payload, 'big')
            self.send(message.piece(index, self.peer.pieces[index]))
        elif msg.type == message.PIECE:
            index = int.from_bytes(msg.payload[:4], 'big')
            piece = msg.payload[4:]
            self.peer.pieces[index] = piece
            with open(self.peer.filename, 'wb') as f:
                f.seek(index * self.peer.piece_size)
                f.write(piece)
            self.peer.arr[index] = True
            self.pieces_received += 1
            for conn in self.peer.connections:
                conn.send(message.have(index))
            self._request_piece()

    def _request_piece(self):
        diff = self.other_arr & ~self.peer.arr
        start_pos = diff.search(bitarray.bitarray('1'))
        if len(start_pos) > 0:
            self.send(message.request(random.choice(start_pos)))

    def _notify_interest(self):
        diff = self.other_arr & ~self.peer.arr
        if diff.count() > 0:
            self.send(message.interested())
        else:
            self.send(message.not_interested())

    def rcv(self):
        raw = b''
        while len(raw) < 4:
            raw += self.sock.recv(4)
        length = 28 if raw == b'P2PF' else int.from_bytes(raw, 'big')
        while len(raw) < 4 + length:
            raw += self.sock.recv(length)
        msg = message.parse(raw)
        print(f'Peer {self.peer.peer_id} received {msg.get_type()} from Peer {self.other_peer_id}')
        return msg

    def send(self, msg):
        self.sock.send(msg)
        print(f'Peer {self.peer.peer_id} sent {message.parse(msg).get_type()} to Peer {self.other_peer_id}')
