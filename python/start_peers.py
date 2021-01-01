import config
import peer

if __name__ == "__main__":
    cfg = config.Config("Common.cfg")
    with open('PeerInfo.txt', 'r') as f:
        peer_info = []
        for line in f.readlines():
            [peer_id, hostname, port, has_file] = line.strip().split(' ')
            port = int(port)
            peer_ = peer.Peer(int(peer_id), hostname, port, int(has_file), cfg, peer_info)
            peer_.start()
            peer_info.append((hostname, port))
