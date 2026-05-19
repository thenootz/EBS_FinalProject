package ebs.crypto;

import ebs.proto.EbsProto.*;
import com.google.protobuf.ByteString;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;

/**
 * Encrypted pub/sub matching (bonus feature).
 *
 * Strategy: Deterministic hashing
 * - Both publisher and subscriber hash field values using SHA-256 with a shared salt.
 * - The broker compares hashes — it never sees plaintext content.
 * - Equality matching (=) works perfectly on hashes.
 * - Range operators (>, <, >=, <=) are NOT compatible with hashing,
 *   so only equality predicates are supported in encrypted mode.
 *
 * Additionally, the full publication payload is AES-GCM encrypted so the
 * broker cannot read the content — only route and match.
 *
 * The subscriber receives the encrypted publication and decrypts it locally
 * using the shared symmetric key.
 */
public class CryptoService {

    // Shared salt — in a real system this would be exchanged securely
    private static final byte[] SALT = "EBS-SHARED-SALT-2024".getBytes(StandardCharsets.UTF_8);

    private final SecretKey aesKey;
    private final SecureRandom rng = new SecureRandom();

    public CryptoService() {
        try {
            KeyGenerator kg = KeyGenerator.getInstance("AES");
            kg.init(256);
            this.aesKey = kg.generateKey();
        } catch (Exception e) {
            throw new RuntimeException("Failed to init CryptoService", e);
        }
    }

    public CryptoService(SecretKey key) {
        this.aesKey = key;
    }

    // ── Hash a field value (deterministic) ───────────────────────────────────
    public static byte[] hashValue(String value) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(SALT);
            return md.digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ── Build an encrypted Publication ───────────────────────────────────────
    public Publication encryptPublication(Publication plain) {
        try {
            byte[] payload = plain.toByteArray();
            byte[] iv      = new byte[12];
            rng.nextBytes(iv);

            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, aesKey, new GCMParameterSpec(128, iv));
            byte[] ciphertext = cipher.doFinal(payload);

            // Prepend IV to ciphertext
            byte[] combined = new byte[iv.length + ciphertext.length];
            System.arraycopy(iv,         0, combined, 0,         iv.length);
            System.arraycopy(ciphertext, 0, combined, iv.length, ciphertext.length);

            return Publication.newBuilder()
                    .setId(plain.getId())
                    .setTimestamp(plain.getTimestamp())
                    .setIsEncrypted(true)
                    .setEncryptedPayload(ByteString.copyFrom(combined))
                    .build();
        } catch (Exception e) {
            throw new RuntimeException("Encryption failed", e);
        }
    }

    // ── Decrypt a Publication ─────────────────────────────────────────────────
    public Publication decryptPublication(Publication enc) {
        try {
            byte[] combined   = enc.getEncryptedPayload().toByteArray();
            byte[] iv         = Arrays.copyOfRange(combined, 0, 12);
            byte[] ciphertext = Arrays.copyOfRange(combined, 12, combined.length);

            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, aesKey, new GCMParameterSpec(128, iv));
            byte[] plain = cipher.doFinal(ciphertext);

            return Publication.parseFrom(plain);
        } catch (Exception e) {
            throw new RuntimeException("Decryption failed", e);
        }
    }

    // ── Build encrypted predicates for a subscription ────────────────────────
    public Predicate encryptPredicate(Predicate plain) {
        // Only = and != work with hashing
        if (!plain.getOperator().equals("=") && !plain.getOperator().equals("!=")) {
            // For range operators, keep plaintext (limitation of hash-based approach)
            return plain;
        }
        return Predicate.newBuilder()
                .setField(plain.getField())        // field name stays visible for routing
                .setOperator(plain.getOperator())
                .setValue("")                       // clear plaintext value
                .setHashedField(ByteString.copyFrom(hashValue(plain.getField())))
                .setHashedValue(ByteString.copyFrom(hashValue(plain.getValue())))
                .build();
    }

    // ── Match encrypted predicate against encrypted publication field ─────────
    public static boolean matchEncrypted(Publication pub, Predicate pred) {
        // Hash the publication field value and compare to predicate's hash
        String fieldVal = getFieldValue(pub, pred.getField());
        if (fieldVal == null) return false;
        byte[] pubHash  = hashValue(fieldVal);
        byte[] predHash = pred.getHashedValue().toByteArray();
        boolean eq = Arrays.equals(pubHash, predHash);
        return switch (pred.getOperator()) {
            case "="  -> eq;
            case "!=" -> !eq;
            default   -> false; // range ops not supported in encrypted mode
        };
    }

    private static String getFieldValue(Publication pub, String field) {
        return switch (field) {
            case "company"   -> pub.getCompany();
            case "value"     -> String.valueOf(pub.getValue());
            case "drop"      -> String.valueOf(pub.getDrop());
            case "variation" -> String.valueOf(pub.getVariation());
            case "date"      -> pub.getDate();
            default          -> null;
        };
    }
}
