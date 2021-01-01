import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.ThreadLocalRandom;

public class Connection implements Runnable {
    public BitSet otherBitarray;
    public int otherPeerId = -1;
    public boolean interested = false;
    public boolean choked = true;
    public int piecesReceived = 0;

    private final boolean initiateHandshake;
    private final Socket socket;
    private final Peer peer;

    private ObjectInputStream in;
    private ObjectOutputStream out;
    private RandomAccessFile raf;

    public Connection(boolean initiateHandshake, Socket socket, Peer peer) {
        this.initiateHandshake = initiateHandshake;
        this.socket = socket;
        this.peer = peer;
        otherBitarray = new BitSet(peer.numPieces);
        otherBitarray.clear();

        if (peer.hasFile == 0) {
            try {
                String path = String.format("./peer_%s/%s", peer.peerId, peer.filename);
                File file = new File(path);
                file.getParentFile().mkdirs();
                file.createNewFile();
                raf = new RandomAccessFile(path, "rw");
            } catch (IOException e) {
                System.err.println(e.getMessage());
            }
        }
    }

    public void run() {
        try {
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
            if (initiateHandshake) {
                send(Message.handshake(peer.peerId));
            }

            while (peer.missingPieces() || peer.neighborsMissingPieces()) {
                respond(rcv());
            }

            in.close();
            out.close();
            socket.close();
            System.out.printf("Peer %s closed connection with Peer %s%n", peer.peerId, otherPeerId);
        } catch (SocketException | EOFException e) {
            System.out.printf("Peer %s-%s connection was closed%n", peer.peerId, otherPeerId);
        } catch (IOException | ClassNotFoundException e) {
            System.err.println(e.toString());
        }
        peer.connections.remove(this);
    }

    private Message rcv() throws IOException, ClassNotFoundException {
        byte[] msg = (byte[]) in.readObject();
        System.out.printf("Peer %s received %s from Peer %s%n", peer.peerId, getType(msg), otherPeerId);
        return Message.parse(msg);
    }

    public void send(byte[] msg) throws IOException {
        out.writeObject(msg);
        System.out.printf("Peer %s sent %s to Peer %s%n", peer.peerId, getType(msg), otherPeerId);
    }

    private String getType(byte[] msg) {
        return Message.parse(msg).type.toString().toLowerCase();
    }

    private void respond(Message msg) throws IOException {
        switch (msg.type) {
            case HANDSHAKE -> {
                otherPeerId = byteArrayToInt(msg.payload);
                if (!initiateHandshake) {
                    send(Message.handshake(peer.peerId));
                }
                synchronized (peer.bitarray) {
                    if (!peer.bitarray.isEmpty()) {
                        send(Message.bitfield(peer.bitarray));
                    }
                }
            }
            case CHOKE -> {}
            case UNCHOKE -> requestPiece();
            case INTERESTED -> interested = true;
            case NOT_INTERESTED -> interested = false;
            case HAVE -> {
                int index = byteArrayToInt(msg.payload);
                otherBitarray.set(index, true);
                notifyInterest();
            }
            case BITFIELD -> {
                otherBitarray = BitSet.valueOf(msg.payload);
                notifyInterest();
            }
            case REQUEST -> {
                int index = byteArrayToInt(msg.payload);
                send(Message.piece(index, peer.pieces.get(index)));
            }
            case PIECE -> {
                int index = byteArrayToInt(Arrays.copyOfRange(msg.payload, 0, 4));
                byte[] piece = Arrays.copyOfRange(msg.payload, 4, msg.payload.length);
                peer.pieces.put(index, piece);
                raf.seek(index * peer.pieceSize);
                raf.write(piece);
                synchronized (peer.bitarray) {
                    peer.bitarray.set(index);
                }
                piecesReceived++;
                for (Connection conn : peer.connections) {
                    conn.send(Message.have(index));
                }
                requestPiece();
            }
        }
    }

    private int byteArrayToInt(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }

    private void requestPiece() throws IOException {
        BitSet diff = missingBitarray();
        int index = -1;
        if (!diff.isEmpty()) {
            while (index < 0 || index > peer.numPieces - 1) {
                index = ThreadLocalRandom.current().nextInt(0, peer.numPieces - 1);
                index = diff.nextSetBit(index);
            }
            send(Message.request(index));
        }
    }

    private void notifyInterest() throws IOException {
        BitSet diff = missingBitarray();
        if (diff.isEmpty()) {
            send(Message.notinterested());
        } else {
            send(Message.interested());
        }
    }

    private BitSet missingBitarray() {
        synchronized (peer.bitarray) {
            BitSet diff = (BitSet) otherBitarray.clone();
            diff.andNot(peer.bitarray);
            return diff;
        }
    }
}
