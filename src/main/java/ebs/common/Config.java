package ebs.common;

import java.util.List;

/**
 * Central configuration for all ports and addresses.
 * In a real deployment these would come from a config file;
 * for simulation everything runs on localhost.
 */
public final class Config {

    // ── Broker ports ──────────────────────────────────────────────────────────
    public static final int BROKER1_PORT = 5001;
    public static final int BROKER2_PORT = 5002;
    public static final int BROKER3_PORT = 5003;

    // ── Subscriber base port (each subscriber listens for notifications) ──────
    public static final int SUBSCRIBER1_PORT = 7001;
    public static final int SUBSCRIBER2_PORT = 7002;
    public static final int SUBSCRIBER3_PORT = 7003;

    // ── Broker addresses ──────────────────────────────────────────────────────
    public record BrokerAddress(String id, String host, int port) {}

    public static final List<BrokerAddress> BROKERS = List.of(
        new BrokerAddress("broker-1", "localhost", BROKER1_PORT),
        new BrokerAddress("broker-2", "localhost", BROKER2_PORT),
        new BrokerAddress("broker-3", "localhost", BROKER3_PORT)
    );

    // ── Timing ────────────────────────────────────────────────────────────────
    /** How often publishers emit a publication (ms). */
    public static final int PUBLISH_INTERVAL_MS   = 200;
    /** How often brokers send heartbeats (ms). */
    public static final int HEARTBEAT_INTERVAL_MS = 2_000;
    /** How long without heartbeat before a broker is considered dead (ms). */
    public static final int BROKER_TIMEOUT_MS     = 60_000;
    /** Initial grace period after startup before failure detection kicks in. */
    public static final int FAILURE_DETECT_GRACE_MS = 30_000;

    // ── Evaluation ────────────────────────────────────────────────────────────
    public static final int  EVAL_SUBSCRIPTIONS   = 10_000;
    public static final long EVAL_FEED_DURATION_MS = 3 * 60 * 1_000L; // 3 minutes

    private Config() {}
}
