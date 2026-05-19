package ebs;

import ebs.common.Matcher;
import ebs.proto.EbsProto.*;

/**
 * Plain-Java tests for Matcher logic.
 * Run with: java -cp out:lib/* ebs.MatcherTest
 *
 * No JUnit dependency — uses assertions and exits with code 1 on failure.
 */
public class MatcherTest {

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        System.out.println("=== Matcher Tests ===\n");

        testStringEquality();
        testStringInequality();
        testDoubleComparison();
        testDateComparison();
        testMultiplePredicates();
        testNoMatch();
        testQuotedValues();

        System.out.printf("%n[Summary] %d passed, %d failed%n", passed, failed);
        if (failed > 0) System.exit(1);
    }

    // ── Test cases ────────────────────────────────────────────────────────────

    static void testStringEquality() {
        Publication pub = pub("Google", 100, 5, 1.5, "2023-06-01");
        Predicate pred  = pred("company", "=", "\"Google\"");
        assertTrue("String equality match", Matcher.matchesPredicate(pub, pred));

        Predicate pred2 = pred("company", "=", "\"Apple\"");
        assertFalse("String equality no match", Matcher.matchesPredicate(pub, pred2));
    }

    static void testStringInequality() {
        Publication pub = pub("Google", 100, 5, 1.5, "2023-06-01");
        Predicate pred  = pred("company", "!=", "\"Apple\"");
        assertTrue("String inequality match (Google != Apple)", Matcher.matchesPredicate(pub, pred));

        Predicate pred2 = pred("company", "!=", "\"Google\"");
        assertFalse("String inequality no match", Matcher.matchesPredicate(pub, pred2));
    }

    static void testDoubleComparison() {
        Publication pub = pub("Tesla", 90.5, 10.0, 0.7, "2022-01-01");

        assertTrue ("value >= 90.0",   Matcher.matchesPredicate(pub, pred("value", ">=",  "90.0")));
        assertTrue ("value <= 91.0",   Matcher.matchesPredicate(pub, pred("value", "<=",  "91.0")));
        assertTrue ("value > 90.0",    Matcher.matchesPredicate(pub, pred("value", ">",   "90.0")));
        assertFalse("value > 91.0",    Matcher.matchesPredicate(pub, pred("value", ">",   "91.0")));
        assertTrue ("value < 100.0",   Matcher.matchesPredicate(pub, pred("value", "<",   "100.0")));
        assertFalse("value = 90.0",    Matcher.matchesPredicate(pub, pred("value", "=",   "90.0")));  // 90.5 != 90.0
        assertTrue ("value = 90.5",    Matcher.matchesPredicate(pub, pred("value", "=",   "90.5")));
        assertTrue ("drop != 5.0",     Matcher.matchesPredicate(pub, pred("drop",  "!=",  "5.0")));
    }

    static void testDateComparison() {
        Publication pub = pub("Tesla", 90.0, 5.0, 1.0, "2023-06-15");

        assertTrue ("date >= 2023-01-01", Matcher.matchesPredicate(pub, pred("date", ">=", "2023-01-01")));
        assertTrue ("date <= 2023-12-31", Matcher.matchesPredicate(pub, pred("date", "<=", "2023-12-31")));
        assertFalse("date >  2023-12-31", Matcher.matchesPredicate(pub, pred("date", ">",  "2023-12-31")));
        assertTrue ("date =  2023-06-15", Matcher.matchesPredicate(pub, pred("date", "=",  "2023-06-15")));
    }

    static void testMultiplePredicates() {
        Publication pub = pub("Google", 95.0, 12.0, 0.5, "2023-03-20");
        Subscription sub = Subscription.newBuilder()
                .setId("sub-1")
                .setSubscriberId("user-1")
                .addPredicates(pred("company", "=", "\"Google\""))
                .addPredicates(pred("value", ">=", "90.0"))
                .addPredicates(pred("variation", "<", "1.0"))
                .build();

        assertTrue("All 3 predicates match", Matcher.matches(pub, sub));
    }

    static void testNoMatch() {
        Publication pub = pub("Microsoft", 50.0, 5.0, 2.0, "2024-01-01");
        Subscription sub = Subscription.newBuilder()
                .setId("sub-2").setSubscriberId("user-1")
                .addPredicates(pred("company", "=", "\"Google\""))
                .addPredicates(pred("value", ">=", "100.0"))
                .build();

        assertFalse("Subscription not satisfied by publication", Matcher.matches(pub, sub));
    }

    static void testQuotedValues() {
        // The generator wraps string values in quotes; matcher must strip them
        Publication pub = pub("Apple", 200.0, 8.0, 0.3, "2022-12-01");
        Predicate withQuotes    = pred("company", "=", "\"Apple\"");
        Predicate withoutQuotes = pred("company", "=", "Apple");
        assertTrue("Quoted value matches",  Matcher.matchesPredicate(pub, withQuotes));
        assertTrue("Plain value matches",   Matcher.matchesPredicate(pub, withoutQuotes));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    static Publication pub(String company, double value, double drop, double variation, String date) {
        return Publication.newBuilder()
                .setId("test")
                .setCompany(company)
                .setValue(value)
                .setDrop(drop)
                .setVariation(variation)
                .setDate(date)
                .setTimestamp(System.currentTimeMillis())
                .build();
    }

    static Predicate pred(String field, String op, String value) {
        return Predicate.newBuilder()
                .setField(field).setOperator(op).setValue(value)
                .build();
    }

    static void assertTrue(String name, boolean cond) {
        if (cond) { passed++; System.out.println("  ✓ " + name); }
        else      { failed++; System.out.println("  ✗ " + name + " — EXPECTED TRUE"); }
    }

    static void assertFalse(String name, boolean cond) {
        if (!cond) { passed++; System.out.println("  ✓ " + name); }
        else       { failed++; System.out.println("  ✗ " + name + " — EXPECTED FALSE"); }
    }
}
