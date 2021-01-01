import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;

public class Message {

    public static String HANDSHAKE = "handshake";
    public static String CHOKE = "choke";
    public static String UNCHOKE = "unchoke";
    public static String INTERESTED = "interested";
    public static String NOT_INTERESTED = "not interested";
    public static String HAVE = "have";
    public static String BITFIELD = "bitfield";
    public static String REQUEST = "request";
    public static String PIECE = "piece";

    private static String P2PFILESHARINGPROJ = "P2PFILESHARINGPROJ";

    public static byte[] handshake(int peerId) {
        byte[] msg = new byte[32];
        byte[] header = P2PFILESHARINGPROJ.getBytes();
        System.arraycopy(header, 0, msg, 0, 18);
        byte[] peerIdBytes = intToByteArray(peerId);
        System.arraycopy(peerIdBytes, 0, msg, 28, 4);
        return msg;
    }

    public static byte[] choke() {
        byte[] msg = new byte[5];
        byte[] msgLength = intToByteArray(1);
        System.arraycopy(msgLength, 0, msg, 0, 4);
        msg[4] = 0x0;
        return msg;
    }

    public static byte[] unchoke() {
        byte[] msg = new byte[5];
        byte[] msgLength = intToByteArray(1);
        System.arraycopy(msgLength, 0, msg, 0, 4);
        msg[4] = 0x1;
        return msg;
    }

    public static byte[] interested() {
        byte[] msg = new byte[5];
        byte[] msgLength = intToByteArray(1);
        System.arraycopy(msgLength, 0, msg, 0, 4);
        msg[4] = 0x2;
        return msg;
    }

    public static byte[] notinterested() {
        byte[] msg = new byte[5];
        byte[] msgLength = intToByteArray(1);
        System.arraycopy(msgLength, 0, msg, 0, 4);
        msg[4] = 0x3;
        return msg;
    }

    public static byte[] have(int index) {
        byte[] msg = new byte[9];
        byte[] msgLength = intToByteArray(index);
        System.arraycopy(msgLength, 0, msg, 0, 4);
        msg[4] = 0x4;
        byte[] payload = intToByteArray(index);
        System.arraycopy(payload, 0, msg, 5, 4);
        return msg;
    }

    public static byte[] bitfield(BitSet bitarray) {
        byte[] msg = new byte[4 + 1 + (bitarray.length() + 7)/8];
        byte[] msgLength = intToByteArray(1 + (bitarray.length() + 7)/8);
        System.arraycopy(msgLength, 0, msg, 0, 4);
        msg[4] = 0x5;
        byte[] payload = bitarray.toByteArray();
        System.arraycopy(payload, 0, msg, 5, payload.length);
        return msg;
    }

    public static byte[] request(int index) {
        byte[] msg = new byte[9];
        byte[] msgLength = intToByteArray(5);
        System.arraycopy(msgLength, 0, msg, 0, 4);
        msg[4] = 0x6;
        byte[] payload = intToByteArray(index);
        System.arraycopy(payload, 0, msg, 5, 4);
        return msg;
    }

    public static byte[] piece(int index, byte[] filePiece) {
        byte[] msg = new byte[9 + filePiece.length];
        byte[] msgLength = intToByteArray(1 + 4 + filePiece.length);
        System.arraycopy(msgLength, 0, msg, 0, 4);
        msg[4] = 0x7;
        byte[] indexPayload = intToByteArray(index);
        System.arraycopy(indexPayload, 0, msg, 5, 4);
        System.arraycopy(filePiece, 0, msg, 9, filePiece.length);
        return msg;
    }

    public static String typeOf(byte[] msg) {
        byte msgType = msg[4];
        byte[] handshakeHeader = Arrays.copyOfRange(msg, 0, 18);
        if (new String(handshakeHeader).equals(P2PFILESHARINGPROJ)) {
            return HANDSHAKE;
        } else if (msgType == 0) {
            return CHOKE;
        } else if (msgType == 1) {
            return UNCHOKE;
        } else if (msgType == 2) {
            return INTERESTED;
        } else if (msgType == 3) {
            return NOT_INTERESTED;
        } else if (msgType == 4) {
            return HAVE;
        } else if (msgType == 5) {
            return BITFIELD;
        } else if (msgType == 6) {
            return REQUEST;
        } else if (msgType == 7) {
            return PIECE;
        } else {
            return "INVALID";
        }
    }

    private static byte[] intToByteArray(int n) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(n);
        return buffer.array();
    }
}
