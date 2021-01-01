import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StartPeers {
    private static final ExecutorService threadPool = Executors.newCachedThreadPool();

    public static void main(String[] args) {
        Config config = new Config("Common.cfg");
        List<Peer> peers = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader("PeerInfo.txt"));
            String line = reader.readLine();
            while (line != null) {
                String[] info = line.strip().split(" ");
                int peerId = toInt(info[0]);
                Peer peer = new Peer(peerId, info[1], toInt(info[2]), toInt(info[3]), config, peers);
                threadPool.submit(peer);
                peers.add(peer);
                line = reader.readLine();
            }
        } catch (IOException e) {
            System.err.println(e.toString());
        }
    }

    private static int toInt(String s) {
        return Integer.parseInt(s);
    }
}
