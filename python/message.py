HANDSHAKE = 10
CHOKE = 0
UNCHOKE = 1
INTERESTED = 2
NOT_INTERESTED = 3
HAVE = 4
BITFIELD = 5
REQUEST = 6
PIECE = 7


class Message:

    def __init__(self, length, type_, payload):
        self.length = length
        self.type = type_
        self.payload = payload

    def get_type(self):
        if self.type == HANDSHAKE:
            return 'handshake'
        elif self.type == CHOKE:
            return 'choke'
        elif self.type == UNCHOKE:
            return 'unchoke'
        elif self.type == INTERESTED:
            return 'interested'
        elif self.type == NOT_INTERESTED:
            return 'not_interested'
        elif self.type == HAVE:
            return 'have'
        elif self.type == BITFIELD:
            return 'bitfield'
        elif self.type == REQUEST:
            return 'request'
        elif self.type == PIECE:
            return 'piece'
        else:
            return 'INVALID'

    def to_bytes(self):
        return self.length.to_bytes(4, 'big') + self.type.to_bytes(1, 'big') + self.payload


def handshake(peer_id):
    return bytes('P2PFILESHARINGPROJ', 'ascii') + b'\0\0\0\0\0\0\0\0\0\0' + peer_id.to_bytes(4, 'big')


def choke():
    return Message(1, CHOKE, b'').to_bytes()


def unchoke():
    return Message(1, UNCHOKE, b'').to_bytes()


def interested():
    return Message(1, INTERESTED, b'').to_bytes()


def not_interested():
    return Message(1, NOT_INTERESTED, b'').to_bytes()


def have(index):
    return Message(1 + 4, HAVE, index.to_bytes(4, 'big')).to_bytes()


def bitfield(arr):
    payload = arr.tobytes()
    return Message(1 + len(payload), BITFIELD, payload).to_bytes()


def request(index):
    return Message(1 + 4, REQUEST, index.to_bytes(4, 'big')).to_bytes()


def piece(index, file_piece):
    payload = index.to_bytes(4, 'big') + file_piece
    return Message(1 + len(payload), PIECE, payload).to_bytes()


def parse(msg):
    if msg[:18] == bytes('P2PFILESHARINGPROJ', 'ascii'):
        return Message(1 + 4, HANDSHAKE, msg[28:32])
    else:
        length = int.from_bytes(msg[:4], 'big')
        return Message(length, msg[4], msg[5:4 + length])
