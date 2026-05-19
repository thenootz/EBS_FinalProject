package ebs;

import ebs.generator.SubscriptionGenerator;
import ebs.proto.EbsProto.*;

import java.util.*;

/**
 * Tests that SubscriptionGenerator produces EXACT frequencies
 * as configured (no random drift).
 */
public class SubscriptionGeneratorTest {

    private static int passed = 0, failed = 0;

    public static void main(String[] args) {
        System.out.println("=== SubscriptionGenerator Tests ===\n");

        testFieldFrequenciesExact();
        testEqualityFrequenciesExact();
        testFactoryAllEquality();
        testFactoryQuarterEquality();

        System.out.printf("%n[Summary] %d passed, %d failed%n", passed, failed);
        if (failed > 0) System.exit(1);
    }

    static void testFieldFrequenciesExact() {
        int N = 10_000;
        Map<String, Double> ff = new LinkedHashMap<>();
        ff.put("company",   0.90);
        ff.put("value",     0.80);
        ff.put("drop",      0.60);
        ff.put("variation", 0.50);
        ff.put("date",      0.40);

        SubscriptionGenerator gen = new SubscriptionGenerator(N, ff, new HashMap<>());

        Map<String, Integer> count = new HashMap<>();
        for (int i = 0; i < N; i++) {
            Subscription sub = gen.next("user");
            for (Predicate p : sub.getPredicatesList()) {
                count.merge(p.getField(), 1, Integer::sum);
            }
        }

        // Tolerance ±1 due to ceiling rounding
        assertWithinOne("company freq",   (int)(0.90 * N), count.getOrDefault("company", 0));
        assertWithinOne("value freq",     (int)(0.80 * N), count.getOrDefault("value", 0));
        assertWithinOne("drop freq",      (int)(0.60 * N), count.getOrDefault("drop", 0));
        assertWithinOne("variation freq", (int)(0.50 * N), count.getOrDefault("variation", 0));
        assertWithinOne("date freq",      (int)(0.40 * N), count.getOrDefault("date", 0));
    }

    static void testEqualityFrequenciesExact() {
        int N = 10_000;
        Map<String, Double> ff = Map.of("company", 0.9, "value", 0.8);
        Map<String, Double> eq = Map.of("company", 0.7, "value", 0.3);

        SubscriptionGenerator gen = new SubscriptionGenerator(N, ff, eq);

        int companyEqCount = 0, companyTotal = 0;
        int valueEqCount   = 0, valueTotal   = 0;
        for (int i = 0; i < N; i++) {
            Subscription sub = gen.next("user");
            for (Predicate p : sub.getPredicatesList()) {
                if (p.getField().equals("company")) {
                    companyTotal++;
                    if (p.getOperator().equals("=")) companyEqCount++;
                }
                if (p.getField().equals("value")) {
                    valueTotal++;
                    if (p.getOperator().equals("=")) valueEqCount++;
                }
            }
        }

        // Expected: 70% of company (0.7 × ceil(0.9 × 10000))
        int expectedCompanyEq = (int) Math.ceil(0.7 * Math.ceil(0.9 * N));
        int expectedValueEq   = (int) Math.ceil(0.3 * Math.ceil(0.8 * N));

        assertWithinOne("company '=' freq", expectedCompanyEq, companyEqCount);
        assertWithinOne("value '=' freq",   expectedValueEq,   valueEqCount);
    }

    static void testFactoryAllEquality() {
        int N = 1_000;
        SubscriptionGenerator gen = SubscriptionGenerator.allEquality(N);
        int companyEq = 0, companyTotal = 0;
        for (int i = 0; i < N; i++) {
            Subscription sub = gen.next("u");
            for (Predicate p : sub.getPredicatesList()) {
                if (p.getField().equals("company")) {
                    companyTotal++;
                    if (p.getOperator().equals("=")) companyEq++;
                }
            }
        }
        assertEquals("allEquality: 100% company = ", companyTotal, companyEq);
    }

    static void testFactoryQuarterEquality() {
        int N = 1_000;
        SubscriptionGenerator gen = SubscriptionGenerator.quarterEquality(N);
        int companyEq = 0, companyTotal = 0;
        for (int i = 0; i < N; i++) {
            Subscription sub = gen.next("u");
            for (Predicate p : sub.getPredicatesList()) {
                if (p.getField().equals("company")) {
                    companyTotal++;
                    if (p.getOperator().equals("=")) companyEq++;
                }
            }
        }
        // Should be ~25%
        double actualRatio = (double) companyEq / companyTotal;
        boolean ok = Math.abs(actualRatio - 0.25) < 0.01;
        if (ok) { passed++; System.out.printf("  ✓ quarterEquality: %.1f%% ≈ 25%%%n", actualRatio * 100); }
        else    { failed++; System.out.printf("  ✗ quarterEquality: %.1f%% deviates from 25%%%n", actualRatio * 100); }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    static void assertWithinOne(String name, int expected, int actual) {
        if (Math.abs(expected - actual) <= 1) {
            passed++; System.out.printf("  ✓ %s: expected %d, got %d%n", name, expected, actual);
        } else {
            failed++; System.out.printf("  ✗ %s: expected %d, got %d%n", name, expected, actual);
        }
    }

    static void assertEquals(String name, int expected, int actual) {
        if (expected == actual) {
            passed++; System.out.printf("  ✓ %s%n", name);
        } else {
            failed++; System.out.printf("  ✗ %s: expected %d, got %d%n", name, expected, actual);
        }
    }
}
