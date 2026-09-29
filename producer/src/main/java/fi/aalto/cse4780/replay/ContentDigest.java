package fi.aalto.cse4780.replay;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * SHA-256 over a partition's records in offset order: key, value and {@code src} header, each
 * length-prefixed. Offsets and timestamps are left out, so two replays of the same rows at
 * different speeds produce the same digest.
 */
final class ContentDigest {

    private final MessageDigest sha256;
    private final ByteBuffer length = ByteBuffer.allocate(Integer.BYTES);
    private long records;

    ContentDigest() {
        try {
            sha256 = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    void add(byte[] key, byte[] value, byte[] source) {
        field(key);
        field(value);
        field(source);
        records++;
    }

    long records() {
        return records;
    }

    /** Hex digest. Ends the computation. */
    String hex() {
        return HexFormat.of().formatHex(sha256.digest());
    }

    private void field(byte[] bytes) {
        byte[] data = bytes == null ? new byte[0] : bytes;
        length.clear();
        length.putInt(bytes == null ? -1 : data.length);
        sha256.update(length.array());
        sha256.update(data);
    }
}
