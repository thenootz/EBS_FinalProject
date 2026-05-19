package ebs;

import ebs.broker.Broker;
import ebs.common.Config;
import ebs.crypto.CryptoService;
import ebs.generator.SubscriptionGenerator;
import ebs.publisher.Publisher;
import ebs.subscriber.Subscriber;

import java.util.*;

/**
 * Main simulation entry point.
 *
 * Starts the full pub/sub system:
 *   - 3 brokers
 *   - 2 publishers
 *   - 3 subscribers
 *
 * Usage:
 *   java -cp target/pubsub-1.0.jar ebs.Main [--encrypted] [--fault-test]
 *
 *   --encrypted   : enable AES-GCM encryption + SHA-256 hash matching (bonus)
 *   --fault-test  : simulate broker failure after 10s (bonus)
 */
public class Main {

    public static void main(String[] args) throws Exception {
        boolean useEncryption = Arrays.asList(args).contains("--encrypted");
        boolean faultTest     = Arrays.asList(args).contains("--fault-test");

        System.out.println("╔══════════════════════════════════════════════════════╗");
        System.out.println("║   EBS Content-Based Pub/Sub System                  ║");
        System.out.println("╚══════════════════════════════════════════════════════╝");
        System.out.println("  Encryption : " + (useEncryption ? "ON (AES-GCM + SHA-256)" : "OFF"));
        System.out.println("  Fault test : " + (faultTest ? "ON (broker-2 will be killed at t=10s)" : "OFF"));
        System.out.println();

        CryptoService crypto = new CryptoService();

        // ── 1. Start brokers ──────────────────────────────────────────────────
        Broker b1 = new Broker("broker-1", Config.BROKER1_PORT, Config.BROKERS);
        Broker b2 = new Broker("broker-2", Config.BROKER2_PORT, Config.BROKERS);
        Broker b3 = new Broker("broker-3", Config.BROKER3_PORT, Config.BROKERS);
        b1.start(); b2.start(); b3.start();
        System.out.println("[System] 3 brokers started");
        Thread.sleep(300);

        // ── 2. Start subscribers ──────────────────────────────────────────────
        Subscriber s1 = new Subscriber("sub-1", Config.SUBSCRIBER1_PORT,
                Config.BROKERS, crypto, useEncryption);
        Subscriber s2 = new Subscriber("sub-2", Config.SUBSCRIBER2_PORT,
                Config.BROKERS, crypto, useEncryption);
        Subscriber s3 = new Subscriber("sub-3", Config.SUBSCRIBER3_PORT,
                Config.BROKERS, crypto, useEncryption);
        s1.startListening(); s2.startListening(); s3.startListening();
        System.out.println("[System] 3 subscribers listening");

        // ── 3. Register subscriptions (100 per subscriber for quick demo) ─────
        Map<String, Double> ff = new LinkedHashMap<>();
        ff.put("company", 0.9); ff.put("value", 0.8);
        ff.put("drop", 0.6);    ff.put("variation", 0.5); ff.put("date", 0.4);
        Map<String, Double> eq = Map.of("company", 0.7, "value", 0.3);

        int subsPerSubscriber = 100;
        SubscriptionGenerator gen = new SubscriptionGenerator(
                subsPerSubscriber * 3, ff, eq);

        s1.registerSubscriptions(subsPerSubscriber, gen);
        s2.registerSubscriptions(subsPerSubscriber, gen);
        s3.registerSubscriptions(subsPerSubscriber, gen);
        System.out.printf("[System] %d subscriptions registered%n", subsPerSubscriber * 3);
        Thread.sleep(500);

        // ── 4. Start publishers ───────────────────────────────────────────────
        Publisher p1 = new Publisher("pub-1", Config.BROKERS, useEncryption, crypto);
        Publisher p2 = new Publisher("pub-2", Config.BROKERS, useEncryption, crypto);
        p1.startAsync(); p2.startAsync();
        System.out.println("[System] 2 publishers started — publishing every "
                + Config.PUBLISH_INTERVAL_MS + "ms");

        // ── 5. Optional: simulate broker failure ──────────────────────────────
        if (faultTest) {
            Thread.sleep(10_000);
            System.out.println("\n⚡ [FaultTest] KILLING broker-2 now!");
            b2.stop();
            System.out.println("   Waiting for broker-1 and broker-3 to detect failure...");
        }

        // ── 6. Run for 30 seconds then print stats ────────────────────────────
        int runSeconds = faultTest ? 40 : 30;
        System.out.printf("%n[System] Running for %d seconds...%n", runSeconds);

        for (int i = 1; i <= runSeconds; i++) {
            Thread.sleep(1_000);
            if (i % 5 == 0) {
                long notifs = s1.notificationsReceived.get()
                            + s2.notificationsReceived.get()
                            + s3.notificationsReceived.get();
                System.out.printf("  t=%2ds | pubs sent: %,d | notifications: %,d%n",
                        i,
                        p1.pubsSent.get() + p2.pubsSent.get(),
                        notifs);
            }
        }

        // ── 7. Stop and print final stats ─────────────────────────────────────
        p1.stop(); p2.stop();
        Thread.sleep(200);

        long totalPubs    = p1.pubsSent.get() + p2.pubsSent.get();
        long totalNotifs  = s1.notificationsReceived.get()
                          + s2.notificationsReceived.get()
                          + s3.notificationsReceived.get();

        System.out.println("\n╔════════════════════════════════════╗");
        System.out.println("║         FINAL STATISTICS           ║");
        System.out.println("╠════════════════════════════════════╣");
        System.out.printf ("║  Publications sent  : %,8d    ║%n", totalPubs);
        System.out.printf ("║  Notifications rcvd : %,8d    ║%n", totalNotifs);
        System.out.printf ("║  Avg latency sub-1  : %8.2f ms ║%n", s1.getAverageLatencyMs());
        System.out.printf ("║  Avg latency sub-2  : %8.2f ms ║%n", s2.getAverageLatencyMs());
        System.out.printf ("║  Avg latency sub-3  : %8.2f ms ║%n", s3.getAverageLatencyMs());
        System.out.println("╚════════════════════════════════════╝");

        s1.stop(); s2.stop(); s3.stop();
        b1.stop(); if (!faultTest) b2.stop(); b3.stop();
    }
}
