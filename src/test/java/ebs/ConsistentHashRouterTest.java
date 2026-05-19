package ebs;

import ebs.common.ConsistentHashRouter;
import ebs.proto.EbsProto.*;

import java.util.*;

/**
 * Tests for ConsistentHashRouter.
 * Run with: java -cp out:lib/* ebs.ConsistentHashRouterTest
 */
public class ConsistentHashRouterTest {

    private static int passed = 0, failed = 0;

    public static void main(String[] args) {
        System.out.println("=== ConsistentHashRouter Tests ===\n");

        testDeterministicMapping();
        testFieldDistribution();
        testRemoveBroker();
        testRouteSubscription();
        testNoBrokers();

        System.out.printf("%n[Summary] %d passed, %d failed%n", passed, failed);
        if (failed > 0) System.exit(1);
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    static void testDeterministicMapping() {
        // Same input → same output across instances
        ConsistentHashRouter r1 = new ConsistentHashRouter(List.of("b1", "b2", "b3"));
        ConsistentHashRouter r2 = new ConsistentHashRouter(List.of("b1", "b2", "b3"));

        for (String field : new String[]{"company", "value", "drop", "variation", "date"}) {
            String a = r1.getBrokerForField(field);
            String b = r2.getBrokerForField(field);
            assertEquals("Deterministic for '" + field + "'", a, b);
        }
    }

    static void testFieldDistribution() {
        ConsistentHashRouter router = new ConsistentHashRouter(List.of("b1", "b2", "b3"));

        Set<String> allFields = new HashSet<>();
        Map<String, Set<String>> brokerFields = new HashMap<>();
        for (String broker : List.of("b1", "b2", "b3")) {
            Set<String> fields = router.getFieldsForBroker(broker);
            brokerFields.put(broker, fields);
            allFields.addAll(fields);
        }

        // All 5 fields must be assigned to exactly one broker
        assertEquals("All 5 fields distributed", 5, allFields.size());

        // No field assigned to more than one broker
        int total = brokerFields.values().stream().mapToInt(Set::size).sum();
        assertEquals("No duplicate field assignment", 5, total);
    }

    static void testRemoveBroker() {
        ConsistentHashRouter router = new ConsistentHashRouter(
                new ArrayList<>(List.of("b1", "b2", "b3")));

        assertEquals("Initial broker count", 3, router.getActiveBrokers().size());

        router.removeBroker("b2");
        assertEquals("After removal count", 2, router.getActiveBrokers().size());

        // Field that was on b2 must now route to b1 or b3
        for (String field : new String[]{"company", "value", "drop", "variation", "date"}) {
            String target = router.getBrokerForField(field);
            assertTrue("Field '" + field + "' routes to b1 or b3 after b2 removed",
                    target.equals("b1") || target.equals("b3"));
        }
    }

    static void testRouteSubscription() {
        ConsistentHashRouter router = new ConsistentHashRouter(List.of("b1", "b2", "b3"));

        Subscription sub = Subscription.newBuilder()
                .setId("test-sub")
                .setSubscriberId("user-1")
                .addPredicates(pred("company", "=", "\"Google\""))
                .addPredicates(pred("value", ">=", "100"))
                .addPredicates(pred("variation", "<", "0.5"))
                .build();

        Map<String, List<Predicate>> routing = router.routeSubscription(sub);

        // All 3 predicates should be routed (no losses)
        int total = routing.values().stream().mapToInt(List::size).sum();
        assertEquals("All predicates routed", 3, total);

        // Each predicate must go to the broker that owns its field
        for (Map.Entry<String, List<Predicate>> entry : routing.entrySet()) {
            String broker = entry.getKey();
            for (Predicate p : entry.getValue()) {
                assertEquals("Predicate '" + p.getField() + "' routed to correct broker",
                        broker, router.getBrokerForField(p.getField()));
            }
        }
    }

    static void testNoBrokers() {
        ConsistentHashRouter router = new ConsistentHashRouter(new ArrayList<>());
        try {
            router.getBrokerForField("company");
            failed++;
            System.out.println("  ✗ Empty router should throw — but didn't");
        } catch (IllegalStateException e) {
            passed++;
            System.out.println("  ✓ Empty router throws IllegalStateException");
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    static Predicate pred(String f, String op, String v) {
        return Predicate.newBuilder().setField(f).setOperator(op).setValue(v).build();
    }

    static void assertEquals(String name, Object expected, Object actual) {
        if (Objects.equals(expected, actual)) {
            passed++; System.out.println("  ✓ " + name);
        } else {
            failed++; System.out.println("  ✗ " + name + " — expected " + expected + " got " + actual);
        }
    }

    static void assertEquals(String name, int expected, int actual) {
        assertEquals(name, (Object) expected, (Object) actual);
    }

    static void assertTrue(String name, boolean cond) {
        if (cond) { passed++; System.out.println("  ✓ " + name); }
        else      { failed++; System.out.println("  ✗ " + name); }
    }
}
