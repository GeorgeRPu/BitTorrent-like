import bitarray
import connection
import message
import multiprocessing as mp
import os
import random
import socket
import repeating


def send_choke_and_unchoke(conn, to_choke, to_unchoke):
    if conn.other_peer_id in to_choke:
        conn.send(message.choke())
        conn.choked = True
    elif conn.other_peer_id in to_unchoke:
        conn.send(message.unchoke())
        conn.choked = False


class Peer(mp.Process):

    def __init__(self, peer_id, hostname, port, has_file, cfg, peer_info):
        super(Peer, self).__init__()
        self.peer_id = peer_id
        self.hostname = hostname
        self.port = port
        self.has_file = has_file
        self.peer_info = peer_info

        self.piece_size = cfg.get_int('PieceSize')
        self.file_size = cfg.get_int('FileSize')
        self.optim_unchoking_int = cfg.get_int('OptimisticUnchokingInterval')
        self.unchoking_int = cfg.get_int('UnchokingInterval')
        self.np = cfg.get_int('NumberOfPreferredNeighbors')

        self.num_pieces = self.file_size // self.piece_size + (0 if self.piece_size % self.file_size == 0 else 1)
        self.arr = bitarray.bitarray(self.num_pieces)
        self.pieces = {}
        self.connections = []
        self.optim_unchoked_id = -1

        if self.has_file:
            self.filename = cfg.get_str('FileName')
            with open(self.filename, 'rb') as f:
                for i in range(self.num_pieces):
                    self.pieces[i] = f.read(self.piece_size)
            self.arr.setall(True)
        else:
            folder = f'./peer_{self.peer_id}'
            if not os.path.exists(folder):
                os.mkdir(folder)
            self.filename = os.path.join(folder, cfg.get_str('FileName'))
            self.arr.setall(False)

    def run(self):
        repeating.Timer(self.optim_unchoking_int, self._optimistic_unchoke).start()
        repeating.Timer(self.unchoking_int, self._unchoke).start()

        for hostname, port in self.peer_info:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((hostname, port))
            self.connections.append(connection.Connection(sock, True, self))
            self.connections[-1].start()

        with socket.socket() as server_sock:
            server_sock.bind((self.hostname, self.port))
            server_sock.listen()
            while True:
                sock, address = server_sock.accept()
                self.connections.append(connection.Connection(sock, False, self))
                self.connections[-1].start()

    def _optimistic_unchoke(self):
        choked_but_interested_ids = [conn.other_peer_id for conn in self.connections if conn.interested and conn.choked]
        if len(choked_but_interested_ids) > 0:
            rand_id = random.choice(choked_but_interested_ids)
            if rand_id != self.optim_unchoked_id:
                for conn in self.connections:
                    send_choke_and_unchoke(conn, [self.optim_unchoked_id], [rand_id])
            self.optim_unchoked_id = rand_id

    def _unchoke(self):
        unchoked_neighbor_ids = [conn.other_peer_id for conn in self.connections if not conn.choked]
        interested_neighbor_ids = [conn.other_peer_id for conn in self.connections if conn.interested]
        useful_conn = [conn for conn in self.connections if conn.pieces_received > 0]
        if self.has_file == 1 and len(unchoked_neighbor_ids) < self.np - 1 and len(interested_neighbor_ids) >= self.np - 1:
            random.shuffle(interested_neighbor_ids)
            to_choke = []
            to_unchoke = interested_neighbor_ids[0:self.np - 1]
        elif len(useful_conn) >= self.np - 1:
            top_connections = sorted(useful_conn, key=lambda conn: conn.pieces_received)
            top_ids = [conn.other_peer_id for conn in top_connections[:self.np - 1]]
            to_choke = list(set(unchoked_neighbor_ids) - set(top_ids))
            to_unchoke = list(set(top_ids) - set(unchoked_neighbor_ids))
        for conn in self.connections:
            conn.pieces_received = 0
            send_choke_and_unchoke(conn, to_choke, to_unchoke)

    def missing_pieces(self):
        return self.arr.count() < self.num_pieces

    def neighbor_missing_pieces(self):
        return any([conn.other_arr.count() < self.num_pieces for conn in self.connections])
