package ebs.common;

import ebs.proto.EbsProto.Subscription;
import ebs.proto.EbsProto.Predicate;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Consistent Hashing Router.
 *
 * Assigns each field name to a broker deterministically using SHA-256.
 * This means the same field always goes to the same broker, regardless
 * of which subscriber or publisher triggers the routing.
 *
 * Field → Broker mapping (with 3 brokers):
 *   hash("company")   % 3 → broker index
 *   hash("value")     % 3 → broker index
 *   etc.
 *
 * When a broker fails, its fields are redistributed to the remaining brokers.
 */
public class ConsistentHashRouter {

    private final List<String> brokerIds;

    public ConsistentHashRouter(List<String> brokerIds) {
        this.brokerIds = new ArrayList<>(brokerIds);
    }

    /**
     * Returns the broker ID responsible for a given field name.
     */
    public String getBrokerForField(String fieldName) {
        if (brokerIds.isEmpty()) throw new IllegalStateException("No brokers available");
        int idx = Math.abs(hash(fieldName)) % brokerIds.size();
        return brokerIds.get(idx);
    }

    /**
     * Returns the set of fields owned by a given broker.
     */
    public Set<String> getFieldsForBroker(String brokerId) {
        Set<String> fields = new HashSet<>();
        for (String field : new String[]{"company", "value", "drop", "variation", "date"}) {
            if (getBrokerForField(field).equals(brokerId)) {
                fields.add(field);
            }
        }
        return fields;
    }

    /**
     * Groups predicates of a subscription by the broker responsible for each field.
     * Returns a map: brokerId → list of predicates for that broker.
     */
    public Map<String, List<Predicate>> routeSubscription(Subscription sub) {
        Map<String, List<Predicate>> routing = new HashMap<>();
        for (Predicate pred : sub.getPredicatesList()) {
            String broker = getBrokerForField(pred.getField());
            routing.computeIfAbsent(broker, k -> new ArrayList<>()).add(pred);
        }
        return routing;
    }

    /**
     * Remove a failed broker and redistribute its fields to remaining brokers.
     */
    public void removeBroker(String brokerId) {
        brokerIds.remove(brokerId);
    }

    public List<String> getActiveBrokers() {
        return Collections.unmodifiableList(brokerIds);
    }

    // ── SHA-256 based hash (deterministic across JVM restarts) ────────────────
    private static int hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));
            // use first 4 bytes as int
            return ((digest[0] & 0xFF) << 24)
                 | ((digest[1] & 0xFF) << 16)
                 | ((digest[2] & 0xFF) <<  8)
                 |  (digest[3] & 0xFF);
        } catch (NoSuchAlgorithmException e) {
            return key.hashCode();
        }
    }
}
