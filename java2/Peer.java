import java.io.FileInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Peer implements Runnable {
    private final ExecutorService threadPool = Executors.newCachedThreadPool();
    private final ScheduledExecutorService scheduledThreadPool = Executors.newScheduledThreadPool(2);

    public final int peerId;
    public final String hostname;
    public final int port;
    public final int hasFile;
    public final String filename;
    public final int pieceSize;
    public final int numPieces;
    public final BitSet bitarray;
    public final Map<Integer, byte[]> pieces = new ConcurrentHashMap<>();
    public final List<Connection> connections = new CopyOnWriteArrayList<>();

    private final List<Peer> peers = new ArrayList<>();
    private final int numPrefNeighbors;
    private final int optimisticUnchokingInterval;
    private final int unchokingInterval;

    private int optimisticallyUnchokedId;

    public Peer(int peerId, String hostname, int port, int hasFile, Config config, List<Peer> peers) {
        this.peerId = peerId;
        this.hostname = hostname;
        this.port = port;
        this.hasFile = hasFile;
        this.peers.addAll(peers);

        numPrefNeighbors = config.getInt("NumberOfPreferredNeighbors");
        optimisticUnchokingInterval = config.getInt("OptimisticUnchokingInterval");
        unchokingInterval = config.getInt("UnchokingInterval");
        pieceSize = config.getInt("PieceSize");
        filename = config.getString("FileName");

        int fileSize = config.getInt("FileSize");
        numPieces = fileSize / pieceSize + (fileSize % pieceSize == 0? 0 : 1);
        bitarray = new BitSet(numPieces);
        bitarray.clear();

        if (hasFile == 1) {
            readFile();
            bitarray.set(0, numPieces, true);
        }
    }

    private void readFile() {
        try {
            final FileInputStream fileInputStream = new FileInputStream(filename);
            for (int i = 0; i < numPieces; i++) {
                byte[] piece = new byte[pieceSize];
                fileInputStream.read(piece);
                pieces.put(i, piece);
            }
        } catch (IOException e) {
            System.err.println(e.toString());
        }
    }

    public void run() {
        try {
            scheduledThreadPool.scheduleAtFixedRate(
                    this::optimisticUnchoke, 0, optimisticUnchokingInterval, TimeUnit.SECONDS);
            scheduledThreadPool.scheduleAtFixedRate(
                    this::unchoke, 0, unchokingInterval, TimeUnit.SECONDS);
            for (Peer peer : peers) {
                Connection conn = new Connection(true, new Socket(peer.hostname, peer.port), this);
                connections.add(conn);
                threadPool.execute(conn);
                System.out.printf("Peer %s connected to Peer %s%n", peerId, peer.peerId);
            }
            ServerSocket serverSocket = new ServerSocket(port);
            while (true) {
                Connection conn = new Connection(false, serverSocket.accept(), this);
                connections.add(conn);
                threadPool.execute(conn);
            }
        } catch (IOException e) {
            System.err.println(e.toString());
        }
    }

    private void optimisticUnchoke() {
        List<Integer> chokedButInterestedIds = connections.stream()
                .filter(conn -> conn.interested && conn.choked).map(conn -> conn.otherPeerId)
                .collect(Collectors.toList());
        if (!chokedButInterestedIds.isEmpty()) {
            int randInt = ThreadLocalRandom.current().nextInt(0, chokedButInterestedIds.size());
            int randId = chokedButInterestedIds.get(randInt);
            if (randId != optimisticallyUnchokedId) {
                for (Connection conn : connections) {
                    sendChokeAndUnchoke(conn, List.of(optimisticallyUnchokedId), List.of(randId));
                }
            }
            optimisticallyUnchokedId = randId;
        }
    }

    private void unchoke() {
        List<Integer> unchokedNeighborIds = connections.stream()
                .filter(conn -> !conn.choked).map(conn -> conn.otherPeerId)
                .collect(Collectors.toList());
        List<Integer> interestedNeighborIds = connections.stream()
                .filter(conn -> conn.interested).map(conn -> conn.otherPeerId)
                .collect(Collectors.toList());
        List<Connection> usefulConnections = connections.stream()
                .filter(conn -> conn.piecesReceived > 0)
                .collect(Collectors.toList());
        List<Integer> toChoke = new ArrayList<>();
        List<Integer> toUnchoke = new ArrayList<>();
        if (hasFile == 1 && unchokedNeighborIds.size() < numPrefNeighbors - 1
                && interestedNeighborIds.size() >= numPrefNeighbors - 1) {
            Collections.shuffle(interestedNeighborIds);
            toUnchoke.addAll(interestedNeighborIds.subList(0, numPrefNeighbors - 1));
        } else if (usefulConnections.size() >= numPrefNeighbors - 1) {
            connections.sort(Comparator.comparingInt(conn -> conn.piecesReceived));
            List<Integer> topIds = connections.subList(0, numPrefNeighbors - 1).stream()
                    .map(conn -> conn.otherPeerId).collect(Collectors.toList());
            toChoke.addAll(difference(unchokedNeighborIds, topIds));
            toUnchoke.addAll(difference(topIds, unchokedNeighborIds));
        }
        for (Connection conn : connections) {
            conn.piecesReceived = 0;
            sendChokeAndUnchoke(conn, toChoke, toUnchoke);
        }
    }

    private void sendChokeAndUnchoke(Connection conn, List<Integer> toChoke, List<Integer> toUnchoke) {
        try {
            if (toChoke.contains(conn.otherPeerId)) {
                conn.send(Message.choke());
                conn.choked = true;
            } else if (toUnchoke.contains(conn.otherPeerId)) {
                conn.send(Message.unchoke());
                conn.choked = false;
            }
        } catch (IOException e) {
            System.err.println(e);
        }
    }

    private List<Integer> difference(List<Integer> a, List<Integer> b) {
        List<Integer> diff = new ArrayList<>(a);
        diff.removeAll(b);
        return diff;
    }

    public boolean missingPieces() {
        synchronized (bitarray) {
            return bitarray.cardinality() < numPieces;
        }
    }

    public boolean neighborsMissingPieces() {
        for (Connection conn : connections) {
            if (conn.otherBitarray.cardinality() < numPieces) {
                return true;
            }
        }
        return false;
    }
}
