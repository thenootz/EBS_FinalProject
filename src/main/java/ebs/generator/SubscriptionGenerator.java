package ebs.generator;

import ebs.proto.EbsProto.*;

import java.time.LocalDate;
import java.util.*;

/**
 * Generates random Subscriptions using the deterministic frequency strategy
 * from the tema practica generator.
 *
 * Supports configurable:
 *   - field frequency (fraction of subscriptions containing each field)
 *   - equality frequency (fraction of subscriptions using = for a field)
 */
public class SubscriptionGenerator {

    private static final String[] COMPANIES = {
        "Google", "Apple", "Microsoft", "Amazon", "Tesla",
        "Meta", "Netflix", "Nvidia", "Intel", "AMD"
    };
    private static final LocalDate DATE_START = LocalDate.of(2020, 1, 1);
    private static final int       DATE_RANGE = 365 * 5;
    private static final String[]  FIELDS     = {"company","value","drop","variation","date"};
    private static final String[]  NUM_OPS_NO_EQ = {">=","<=",">","<"};

    private final Map<String, Double> fieldFreq;
    private final Map<String, Double> eqFreq;
    private final Random rng = new Random();

    // Pre-computed slot arrays for deterministic distribution
    private Map<String, int[]>     fieldSlots;
    private Map<String, Set<Integer>> eqSlotSets;
    private final int total;
    private int currentIndex = 0;

    public SubscriptionGenerator(int total,
                                  Map<String, Double> fieldFreq,
                                  Map<String, Double> eqFreq) {
        this.total     = total;
        this.fieldFreq = fieldFreq;
        this.eqFreq    = eqFreq;
        buildSlots();
    }

    private void buildSlots() {
        fieldSlots  = new HashMap<>();
        eqSlotSets  = new HashMap<>();
        Random slotRng = new Random(42);

        for (String field : FIELDS) {
            Double freq = fieldFreq.get(field);
            if (freq == null || freq <= 0) continue;
            int count   = Math.min((int) Math.ceil(freq * total), total);
            int[] indices = new int[total];
            for (int i = 0; i < total; i++) indices[i] = i;
            shuffleArray(indices, slotRng);
            int[] slots = Arrays.copyOf(indices, count);
            Arrays.sort(slots);
            fieldSlots.put(field, slots);
        }

        Random eqRng = new Random(99);
        for (Map.Entry<String, Double> e : eqFreq.entrySet()) {
            String field  = e.getKey();
            int[]  slots  = fieldSlots.get(field);
            if (slots == null) continue;
            int count     = Math.min((int) Math.ceil(e.getValue() * slots.length), slots.length);
            int[] copy    = Arrays.copyOf(slots, slots.length);
            shuffleArray(copy, eqRng);
            Set<Integer> eqSet = new HashSet<>();
            for (int i = 0; i < count; i++) eqSet.add(copy[i]);
            eqSlotSets.put(field, eqSet);
        }
    }

    /**
     * Generate the next subscription. Call up to `total` times.
     */
    public Subscription next(String subscriberId) {
        int idx = currentIndex++;
        Subscription.Builder sub = Subscription.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setSubscriberId(subscriberId);

        for (String field : FIELDS) {
            int[] slots = fieldSlots.get(field);
            if (slots == null) continue;
            if (Arrays.binarySearch(slots, idx) < 0) continue;

            Set<Integer> eqSet  = eqSlotSets.get(field);
            boolean      forceEq = eqSet != null && eqSet.contains(idx);
            sub.addPredicates(buildPredicate(field, forceEq));
        }

        return sub.build();
    }

    private Predicate buildPredicate(String field, boolean forceEq) {
        return switch (field) {
            case "company" -> Predicate.newBuilder()
                    .setField(field)
                    .setOperator(forceEq ? "=" : "!=")
                    .setValue("\"" + COMPANIES[rng.nextInt(COMPANIES.length)] + "\"")
                    .build();
            case "value" -> Predicate.newBuilder()
                    .setField(field)
                    .setOperator(forceEq ? "=" : pickOp())
                    .setValue(String.format("%.2f", rnd(10.0, 500.0)))
                    .build();
            case "drop" -> Predicate.newBuilder()
                    .setField(field)
                    .setOperator(forceEq ? "=" : pickOp())
                    .setValue(String.format("%.2f", rnd(0.0, 50.0)))
                    .build();
            case "variation" -> Predicate.newBuilder()
                    .setField(field)
                    .setOperator(forceEq ? "=" : pickOp())
                    .setValue(String.format("%.4f", rnd(-5.0, 5.0)))
                    .build();
            case "date" -> Predicate.newBuilder()
                    .setField(field)
                    .setOperator(forceEq ? "=" : pickOp())
                    .setValue(DATE_START.plusDays(rng.nextInt(DATE_RANGE)).toString())
                    .build();
            default -> throw new IllegalArgumentException(field);
        };
    }

    private String pickOp() { return NUM_OPS_NO_EQ[rng.nextInt(NUM_OPS_NO_EQ.length)]; }
    private double rnd(double min, double max) { return min + (max - min) * rng.nextDouble(); }

    private static void shuffleArray(int[] arr, Random r) {
        for (int i = arr.length - 1; i > 0; i--) {
            int j = r.nextInt(i + 1);
            int t = arr[i]; arr[i] = arr[j]; arr[j] = t;
        }
    }

    // Convenience factory: 100% equality on company
    public static SubscriptionGenerator allEquality(int total) {
        Map<String, Double> ff = new LinkedHashMap<>();
        ff.put("company", 0.9); ff.put("value", 0.8);
        ff.put("drop", 0.6);    ff.put("variation", 0.5); ff.put("date", 0.4);
        Map<String, Double> eq = Map.of("company", 1.0);
        return new SubscriptionGenerator(total, ff, eq);
    }

    // Convenience factory: 25% equality on company
    public static SubscriptionGenerator quarterEquality(int total) {
        Map<String, Double> ff = new LinkedHashMap<>();
        ff.put("company", 0.9); ff.put("value", 0.8);
        ff.put("drop", 0.6);    ff.put("variation", 0.5); ff.put("date", 0.4);
        Map<String, Double> eq = Map.of("company", 0.25);
        return new SubscriptionGenerator(total, ff, eq);
    }
}
