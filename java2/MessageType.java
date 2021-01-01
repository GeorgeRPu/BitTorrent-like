public enum MessageType {
    HANDSHAKE(10),
    CHOKE(0),
    UNCHOKE(1),
    INTERESTED(2),
    NOT_INTERESTED(3),
    HAVE(4),
    BITFIELD(5),
    REQUEST(6),
    PIECE(7);

    public final int code;

    MessageType(int code) {
        this.code = code;
    }

    public static MessageType valueOf(byte b) {
        for (MessageType type : MessageType.values()) {
            if (type.code == b) {
                return type;
            }
        }
        return null;
    }
}
