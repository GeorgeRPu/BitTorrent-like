import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;

public class Message {
    public static String P2PFILESHARINGPROJ = "P2PFILESHARINGPROJ";

    public final int length;
    public final MessageType type;
    public final byte[] payload;

    public Message(int length, MessageType type, byte[] payload) {
        this.length = length;
        this.type = type;
        this.payload = payload;
    }

    public byte[] toBytes() {
        return concat(intToBytes(this.length), new byte[] {(byte) this.type.code}, payload);
    }

    private static byte[] concat(byte[] first, byte[]... rest) {
        int totalLength = first.length;
        for (byte[] array : rest) {
            totalLength += array.length;
        }
        byte[] result = Arrays.copyOf(first, totalLength);
        int offset = first.length;
        for (byte[] array : rest) {
            System.arraycopy(array, 0, result, offset, array.length);
            offset += array.length;
        }
        return result;
    }

    public static byte[] handshake(int peerId) {
        return concat(P2PFILESHARINGPROJ.getBytes(), new byte[10], intToBytes(peerId));
    }

    public static byte[] choke()  {
        return new Message(1, MessageType.CHOKE, new byte[] {}).toBytes();
    }

    public static byte[] unchoke() {
        return new Message(1, MessageType.UNCHOKE, new byte[] {}).toBytes();
    }

    public static byte[] interested()  {
        return new Message(1, MessageType.INTERESTED, new byte[] {}).toBytes();
    }

    public static byte[] notinterested()  {
        return new Message(1, MessageType.NOT_INTERESTED, new byte[] {}).toBytes();
    }

    public static byte[] have(int index)  {
        return new Message(5, MessageType.HAVE, intToBytes(index)).toBytes();
    }

    public static byte[] bitfield(BitSet bitarray) {
        return new Message(5 + (bitarray.length() + 7)/8, MessageType.BITFIELD, bitarray.toByteArray()).toBytes();
    }

    public static byte[] request(int index) {
        return new Message(5, MessageType.REQUEST, intToBytes(index)).toBytes();
    }

    public static byte[] piece(int index, byte[] filePiece) {
        return new Message(5, MessageType.PIECE, concat(intToBytes(index), filePiece)).toBytes();
    }

    public static byte[] intToBytes(int n) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(n);
        return byteBuffer.array();
    }

    public static Message parse(byte[] msg) {
        if (new String(Arrays.copyOfRange(msg, 0, 18)).equals(P2PFILESHARINGPROJ)) {
            return new Message(1, MessageType.HANDSHAKE, Arrays.copyOfRange(msg, 28, 32));
        } else {
            return new Message(bytesToInt(Arrays.copyOfRange(msg, 0, 4)),
                               MessageType.valueOf(msg[4]),
                               Arrays.copyOfRange(msg, 5, msg.length));
        }
    }

    private static int bytesToInt(byte[] b) {
        return ByteBuffer.wrap(b).getInt();
    }
}
