package ebs;

import ebs.crypto.CryptoService;
import ebs.proto.EbsProto.*;
import com.google.protobuf.ByteString;

import java.util.Arrays;

/**
 * Tests for CryptoService — encryption/decryption and hash-based matching.
 */
public class CryptoServiceTest {

    private static int passed = 0, failed = 0;

    public static void main(String[] args) {
        System.out.println("=== CryptoService Tests ===\n");

        testPublicationRoundTrip();
        testHashDeterministic();
        testEncryptedPredicateMatch();
        testEncryptedPredicateNoMatch();
        testRangeOperatorNotEncrypted();

        System.out.printf("%n[Summary] %d passed, %d failed%n", passed, failed);
        if (failed > 0) System.exit(1);
    }

    static void testPublicationRoundTrip() {
        CryptoService crypto = new CryptoService();
        Publication plain = Publication.newBuilder()
                .setId("pub-1")
                .setCompany("Tesla")
                .setValue(123.45)
                .setDrop(8.0)
                .setVariation(1.5)
                .setDate("2023-06-15")
                .setTimestamp(1234567890L)
                .build();

        Publication encrypted = crypto.encryptPublication(plain);
        assertTrue("Encrypted is flagged as encrypted",      encrypted.getIsEncrypted());
        assertTrue("Encrypted has payload",                  !encrypted.getEncryptedPayload().isEmpty());
        assertEquals("Encrypted preserves id",               plain.getId(), encrypted.getId());

        Publication decrypted = crypto.decryptPublication(encrypted);
        assertEquals("Decrypted company",   plain.getCompany(), decrypted.getCompany());
        assertEquals("Decrypted value",     plain.getValue(),   decrypted.getValue());
        assertEquals("Decrypted date",      plain.getDate(),    decrypted.getDate());
    }

    static void testHashDeterministic() {
        // Same value → same hash always
        byte[] h1 = CryptoService.hashValue("Google");
        byte[] h2 = CryptoService.hashValue("Google");
        assertTrue("Same input → same hash", Arrays.equals(h1, h2));

        // Different value → different hash
        byte[] h3 = CryptoService.hashValue("Apple");
        assertTrue("Different inputs → different hashes", !Arrays.equals(h1, h3));

        // SHA-256 always 32 bytes
        assertEquals("Hash length is 32 bytes", 32, h1.length);
    }

    static void testEncryptedPredicateMatch() {
        Publication pub = Publication.newBuilder()
                .setId("p1")
                .setCompany("Google")
                .setValue(100.0)
                .build();

        // Build predicate with hash of "Google"
        Predicate pred = Predicate.newBuilder()
                .setField("company")
                .setOperator("=")
                .setValue("")
                .setHashedValue(ByteString.copyFrom(CryptoService.hashValue("Google")))
                .build();

        assertTrue("Encrypted = match", CryptoService.matchEncrypted(pub, pred));
    }

    static void testEncryptedPredicateNoMatch() {
        Publication pub = Publication.newBuilder()
                .setCompany("Microsoft")
                .build();

        Predicate pred = Predicate.newBuilder()
                .setField("company")
                .setOperator("=")
                .setHashedValue(ByteString.copyFrom(CryptoService.hashValue("Google")))
                .build();

        assertFalse("Encrypted = no match", CryptoService.matchEncrypted(pub, pred));

        // != also works
        Predicate predNeq = pred.toBuilder().setOperator("!=").build();
        assertTrue("Encrypted != match (Microsoft != Google)",
                CryptoService.matchEncrypted(pub, predNeq));
    }

    static void testRangeOperatorNotEncrypted() {
        // Range operators are kept plaintext (limitation documented)
        CryptoService crypto = new CryptoService();
        Predicate plain = Predicate.newBuilder()
                .setField("value").setOperator(">=").setValue("90.0").build();

        Predicate encrypted = crypto.encryptPredicate(plain);
        // For range ops, the predicate should be returned unchanged
        assertEquals("Range op preserves value",
                plain.getValue(), encrypted.getValue());
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    static void assertTrue(String name, boolean cond) {
        if (cond) { passed++; System.out.println("  ✓ " + name); }
        else      { failed++; System.out.println("  ✗ " + name); }
    }

    static void assertFalse(String name, boolean cond) { assertTrue(name, !cond); }

    static void assertEquals(String name, Object expected, Object actual) {
        if (java.util.Objects.equals(expected, actual)) {
            passed++; System.out.println("  ✓ " + name);
        } else {
            failed++; System.out.println("  ✗ " + name + " — expected " + expected + " got " + actual);
        }
    }

    static void assertEquals(String name, int e, int a) { assertEquals(name, (Object)e, (Object)a); }
    static void assertEquals(String name, double e, double a) { assertEquals(name, (Object)e, (Object)a); }
}
