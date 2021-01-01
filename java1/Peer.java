import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class Peer implements Runnable {
    private static final int RCV_TIMEOUT = 1000;

    private final ExecutorService threadPool = Executors.newCachedThreadPool();

    public final int peerId;
    public final String hostname;
    public final int port;
    private final int hasFile;
    private final int numPieces;
    private final int numPrefNeighbors;
    private final int optimisticUnchokingInterval;
    private final int unchokingInterval;
    private final int pieceSize;
    private final String filename;
    private final BitSet bitarray;
    private final List<Peer> neighborsToConnect;

    private final Map<Integer, BitSet> neighborBitarrays = new ConcurrentHashMap<>();
    private final Map<Integer, byte[]> pieces = new ConcurrentHashMap<>();
    private final List<Integer> interestedNeighborIds = new ArrayList<>();
    private final Set<Integer> unchokedNeighborIds = ConcurrentHashMap.newKeySet();
    private final Set<Integer> toSendUnchokeIds = ConcurrentHashMap.newKeySet();
    private final Set<Integer> toSendChokeIds = ConcurrentHashMap.newKeySet();
    private final Map<Integer, Queue<Integer>> haveQueues = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> piecesReceived = new ConcurrentHashMap<>();

    private long lastOptimistic = 0;
    private long lastUnchoked = 0;
    private int optimisticallyUnchokedId;

    public Peer(int peerId, String hostname, int port, int hasFile, Config config, List<Peer> peers) {
        this.peerId = peerId;
        this.hostname = hostname;
        this.port = port;
        this.hasFile = hasFile;
        this.numPrefNeighbors = config.getInt("NumberOfPreferredNeighbors");
        this.optimisticUnchokingInterval = config.getInt("OptimisticUnchokingInterval");
        this.unchokingInterval = config.getInt("UnchokingInterval");
        this.pieceSize = config.getInt("PieceSize");
        this.filename = config.getString("FileName");
        this.neighborsToConnect = new ArrayList<>(peers);

        int fileSize = config.getInt("FileSize");
        numPieces = fileSize / pieceSize + (fileSize % pieceSize == 0? 0 : 1);
        bitarray = new BitSet(numPieces);
        bitarray.clear();
        if (hasFile == 1) {
            readFile(numPieces);
            bitarray.set(0, numPieces, true);
        }
    }

    private void readFile(int numPieces) {
        try {
            FileInputStream in = new FileInputStream(filename);
            for (int i = 0; i < numPieces; i++) {
                byte[] piece = new byte[pieceSize];
                in.read(piece);
                pieces.put(i, piece);
            }
        } catch (IOException e) {
            System.err.println(e.toString());
        }
    }

    public void run() {
        for (Peer peer : neighborsToConnect) {
            threadPool.execute(new ConnectionHandler(peer));
            System.out.printf("Peer %s connected to Peer %s%n", peerId, peer.peerId);
        }

        try {
            ServerSocket serverSocket = new ServerSocket(port);
            while (true) {
                threadPool.execute(new ServerHandler(serverSocket.accept()));
            }
        } catch (IOException e) {
            System.err.println(e.toString());
        }
    }

    public boolean missingPieces() {
        synchronized (bitarray) {
            return bitarray.cardinality() < numPieces;
        }
    }

    public boolean neighborsMissingPieces() {
        for (BitSet otherBitarray : neighborBitarrays.values()) {
            if (otherBitarray.cardinality() < numPieces) {
                return true;
            }
        }
        return false;
    }

    private class Handler implements Runnable {
        int otherPeerId = -1;
        ObjectInputStream in;
        ObjectOutputStream out;
        RandomAccessFile raf;
        Socket socket;
        boolean initiateHandshake;

        public Handler(boolean initiateHandshake) {
            if (missingPieces()) {
                try {
                    String path = String.format("./peer_%s/%s", peerId, filename);
                    File file = new File(path);
                    file.getParentFile().mkdirs();
                    file.createNewFile();
                    raf = new RandomAccessFile(path, "rw");
                } catch (IOException e) {
                    System.err.println(e.toString());
                }
            }
            this.initiateHandshake = initiateHandshake;
        }

        public void run() {
            try {
                socket.setSoTimeout(RCV_TIMEOUT);
                out = new ObjectOutputStream(socket.getOutputStream());
                if (initiateHandshake) {
                    send(Message.handshake(peerId));
                }
                in = new ObjectInputStream(socket.getInputStream());

                while (missingPieces() || neighborsMissingPieces() || neighborBitarrays.isEmpty()) {
                    unchoke();
                    sendHaves();
                    byte[] msg = rcv();
                    if (msg != null) {
                        respond(msg);
                    }
                }

                in.close();
                out.close();
                socket.close();
                System.out.printf("Peer %s closed connection with Peer %s%n", peerId, otherPeerId);
            } catch (SocketException | EOFException e) {
                System.out.printf("Peer %s-%s connection was closed%n", peerId, otherPeerId);
            } catch (IOException | ClassNotFoundException e) {
                System.err.println(e.toString());
            }
        }

        private void respond(byte[] msg) throws IOException {
            String msgType = Message.typeOf(msg);
            if (msgType.equals(Message.HANDSHAKE)) {
                otherPeerId = byteArrayToInt(Arrays.copyOfRange(msg, 28, 32));
                haveQueues.putIfAbsent(otherPeerId, new ConcurrentLinkedQueue<>());
                if (!initiateHandshake) {
                    send(Message.handshake(peerId));
                }
                synchronized (bitarray) {
                    if (!bitarray.isEmpty()) {
                        send(Message.bitfield(bitarray));
                    }
                }
            } else if (msgType.equals(Message.HAVE)) {
                int index = byteArrayToInt(Arrays.copyOfRange(msg, 5, 9));
                neighborBitarrays.putIfAbsent(otherPeerId, new BitSet(numPieces));
                neighborBitarrays.get(otherPeerId).set(index, true);
                notifyInterest();
            } else if (msgType.equals(Message.BITFIELD)) {
                BitSet otherBitarray = BitSet.valueOf(Arrays.copyOfRange(msg, 5, msg.length));
                neighborBitarrays.put(otherPeerId, otherBitarray);
                notifyInterest();
            } else if (msgType.equals(Message.INTERESTED)) {
                synchronized (interestedNeighborIds) {
                    interestedNeighborIds.add(otherPeerId);
                }
            } else if (msgType.equals(Message.NOT_INTERESTED)) {
                synchronized (interestedNeighborIds) {
                    interestedNeighborIds.removeIf(id -> id == otherPeerId);
                }
            } else if (msgType.equals(Message.CHOKE)) {

            } else if (msgType.equals(Message.UNCHOKE)) {
                requestPiece();
            } else if (msgType.equals(Message.REQUEST)) {
                int index = byteArrayToInt(Arrays.copyOfRange(msg, 5, 9));
                send(Message.piece(index, pieces.get(index)));
            } else if (msgType.equals(Message.PIECE)) {
                int index = byteArrayToInt(Arrays.copyOfRange(msg, 5, 9));
                byte[] piece = Arrays.copyOfRange(msg, 9, msg.length);
                pieces.put(index, piece);
                raf.seek(index * pieceSize);
                raf.write(piece);
                synchronized (bitarray) {
                    bitarray.set(index);
                }
                piecesReceived.putIfAbsent(otherPeerId, 0);
                piecesReceived.put(otherPeerId, piecesReceived.get(otherPeerId) + 1);
                for (Queue<Integer> queue : haveQueues.values()) {
                    queue.add(index);
                }
                requestPiece();
            }
        }

        private void unchoke() throws IOException {
            synchronized (interestedNeighborIds) {
                synchronized (bitarray) {
                    if (hasFile == 1 && unchokedNeighborIds.size() < numPrefNeighbors - 1
                            && interestedNeighborIds.size() >= numPrefNeighbors) {
                        Collections.shuffle(interestedNeighborIds);
                        unchokedNeighborIds.addAll(interestedNeighborIds.subList(0, numPrefNeighbors - 1));
                        toSendUnchokeIds.addAll(unchokedNeighborIds);
                    }
                    List<Integer> chokedButInterestedIds = new ArrayList<>(interestedNeighborIds);
                    chokedButInterestedIds.removeAll(unchokedNeighborIds);

                    long sinceLastOptimistic = (System.currentTimeMillis() - lastOptimistic) / 1000;
                    if (sinceLastOptimistic >= optimisticUnchokingInterval && interestedNeighborIds.size() >= 1) {
                        int randInt = ThreadLocalRandom.current().nextInt(0, chokedButInterestedIds.size());
                        int randId = chokedButInterestedIds.get(randInt);
                        if (randId != optimisticallyUnchokedId) {
                            toSendUnchokeIds.add(randId);
                            toSendChokeIds.add(optimisticallyUnchokedId);
                            optimisticallyUnchokedId = randId;
                        }
                        lastOptimistic = System.currentTimeMillis();
                    }
                    long sinceLastUnchoked = (System.currentTimeMillis() - lastUnchoked) / 1000;
                    if (sinceLastUnchoked >= unchokingInterval && piecesReceived.size() >= numPrefNeighbors - 1) {
                        List<Entry<Integer, Integer>> entries = new ArrayList<>(piecesReceived.entrySet());
                        entries.sort(Entry.comparingByValue());
                        Set<Integer> topIds = entries.subList(0,numPrefNeighbors - 1)
                                .stream().map(Entry::getKey).collect(Collectors.toSet());
                        toSendChokeIds.addAll(difference(unchokedNeighborIds, topIds));
                        toSendUnchokeIds.addAll(difference(topIds, unchokedNeighborIds));
                        unchokedNeighborIds.clear();
                        unchokedNeighborIds.addAll(topIds);
                        piecesReceived.clear();
                        lastUnchoked = System.currentTimeMillis();
                    }
                    if (toSendUnchokeIds.contains(otherPeerId)) {
                        send(Message.unchoke());
                        toSendUnchokeIds.remove(otherPeerId);
                    }
                    if (toSendChokeIds.contains(otherPeerId)) {
                        send(Message.choke());
                        toSendUnchokeIds.remove(otherPeerId);
                    }
                }
            }
        }

        private void sendHaves() throws IOException {
            Queue<Integer> queue = haveQueues.get(otherPeerId);
            while (queue != null && !queue.isEmpty()) {
                send(Message.have(queue.remove()));
            }
        }

        private byte[] rcv() throws IOException, ClassNotFoundException {
            try {
                byte[] msg = (byte[]) in.readObject();
                System.out.printf("Peer %s received %s from Peer %s%n",
                        peerId, Message.typeOf(msg), otherPeerId);
                return msg;
            } catch (SocketTimeoutException e) {
                return null;
            }
        }

        private void send(byte[] msg) throws IOException {
            out.writeObject(msg);
            System.out.printf("Peer %s sent %s to Peer %s%n", peerId, Message.typeOf(msg), otherPeerId);
        }

        private int byteArrayToInt(byte[] bytes) {
            return ByteBuffer.wrap(bytes).getInt();
        }

        private void notifyInterest() throws IOException {
            BitSet diff = missingBitArray();
            if (diff.isEmpty()) {
                send(Message.notinterested());
            } else {
                send(Message.interested());
            }
        }

        private void requestPiece() throws IOException {
            BitSet diff = missingBitArray();
            int index = -1;
            if (!diff.isEmpty()) {
                while (index < 0 || index > numPieces - 1) {
                    index = ThreadLocalRandom.current().nextInt(0, numPieces - 1);
                    index = diff.nextSetBit(index);
                }
                send(Message.request(index));
            }
        }

        private BitSet missingBitArray() {
            synchronized (bitarray) {
                if (!neighborBitarrays.containsKey(otherPeerId)) {
                    BitSet empty = new BitSet(numPieces);
                    empty.clear();
                    return empty;
                }
                BitSet diff = (BitSet) neighborBitarrays.get(otherPeerId).clone();
                diff.andNot(bitarray);
                return diff;
            }
        }

        private Set<Integer> difference(Set<Integer> a, Set<Integer> b) {
            Set<Integer> diff = new HashSet<>(a);
            diff.removeAll(b);
            return diff;
        }
    }

    private class ConnectionHandler extends Peer.Handler {

        public ConnectionHandler(Peer peer) {
            super(true);
            try {
                socket = new Socket(peer.hostname, peer.port);
            } catch (IOException e) {
                System.err.println(e.toString());
            }
        }
    }

    private class ServerHandler extends Peer.Handler {

        public ServerHandler(Socket socket) {
            super(false);
            this.socket = socket;
        }
    }
}
