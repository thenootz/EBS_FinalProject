package ebs;

import ebs.broker.Broker;
import ebs.common.Config;
import ebs.crypto.CryptoService;
import ebs.generator.SubscriptionGenerator;
import ebs.publisher.Publisher;
import ebs.subscriber.Subscriber;

import java.io.PrintWriter;
import java.util.*;

/**
 * EvalHarness — runs both scenarios (100% eq and 25% eq) back-to-back.
 * Configurable feed duration via system property -Dfeed.seconds=30 (default 180).
 */
public class EvalHarness {

    static final int TOTAL_SUBS =
            Integer.parseInt(System.getProperty("total.subs", "10000"));
    static final int FEED_SECONDS =
            Integer.parseInt(System.getProperty("feed.seconds", "180"));

    public static void main(String[] args) throws Exception {
        long feedMs = FEED_SECONDS * 1_000L;
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.printf ("║   EBS EVALUATION — %d subs, %d-second feed                  %n",
                TOTAL_SUBS, FEED_SECONDS);
        System.out.println("╚══════════════════════════════════════════════════════════════╝");

        System.out.println("\n━━━ SCENARIO A: 100% equality on 'company' ━━━");
        EvalResult r100 = runScenario(SubscriptionGenerator.allEquality(TOTAL_SUBS), feedMs);

        Thread.sleep(8_000); // allow ports/threads to fully release between scenarios

        System.out.println("\n━━━ SCENARIO B:  25% equality on 'company' ━━━");
        EvalResult r25  = runScenario(SubscriptionGenerator.quarterEquality(TOTAL_SUBS), feedMs);

        printReport(r100, r25, FEED_SECONDS);
        writeCsv(r100, r25);
    }

    // ─────────────────────────────────────────────────────────────────────────
    static EvalResult runScenario(SubscriptionGenerator gen, long feedMs) throws Exception {
        CryptoService crypto = new CryptoService();

        // ── start brokers ─────────────────────────────────────────────────────
        List<Broker> brokers = new ArrayList<>();
        for (var ba : Config.BROKERS) {
            Broker b = new Broker(ba.id(), ba.port(), Config.BROKERS);
            b.start();
            brokers.add(b);
        }
        Thread.sleep(500);

        // ── start subscribers ─────────────────────────────────────────────────
        List<Subscriber> subs = List.of(
            new Subscriber("sub-1", Config.SUBSCRIBER1_PORT, Config.BROKERS, crypto, false),
            new Subscriber("sub-2", Config.SUBSCRIBER2_PORT, Config.BROKERS, crypto, false),
            new Subscriber("sub-3", Config.SUBSCRIBER3_PORT, Config.BROKERS, crypto, false)
        );
        for (Subscriber s : subs) s.startListening();

        // ── register 10 000 subscriptions, balanced across subscribers ────────
        long regStart = System.currentTimeMillis();
        int perSub = TOTAL_SUBS / subs.size();
        for (Subscriber s : subs) {
            s.registerSubscriptions(perSub, gen);
        }
        long regMs = System.currentTimeMillis() - regStart;
        Thread.sleep(2_000); // let registrations settle through the broker network

        // ── start publishers ──────────────────────────────────────────────────
        List<Publisher> pubs = List.of(
            new Publisher("pub-1", Config.BROKERS, false, crypto),
            new Publisher("pub-2", Config.BROKERS, false, crypto)
        );
        for (Publisher p : pubs) p.startAsync();

        // ── feed ──────────────────────────────────────────────────────────────
        long feedStart = System.currentTimeMillis();
        Thread.sleep(feedMs);
        for (Publisher p : pubs) p.stop();
        Thread.sleep(2_000); // drain in-flight notifications

        // ── collect ───────────────────────────────────────────────────────────
        long totalPubs   = pubs.stream().mapToLong(p -> p.pubsSent.get()).sum();
        long totalNotifs = subs.stream().mapToLong(s -> s.notificationsReceived.get()).sum();
        double avgLat    = subs.stream().mapToDouble(Subscriber::getAverageLatencyMs)
                               .filter(l -> l > 0).average().orElse(0);

        // ── shutdown ──────────────────────────────────────────────────────────
        for (Subscriber s : subs) s.stop();
        for (Broker b : brokers) b.stop();
        ebs.common.PersistentSender.closeAll();
        Thread.sleep(500);

        return new EvalResult(totalPubs, totalNotifs, avgLat, regMs, feedMs);
    }

    // ─────────────────────────────────────────────────────────────────────────
    static void printReport(EvalResult r100, EvalResult r25, int feedSec) {
        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║                  EVALUATION REPORT                          ║");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.printf ("║  Subscriptions registered per scenario : %,6d              ║%n", TOTAL_SUBS);
        System.out.printf ("║  Feed duration                         : %4d s              ║%n", feedSec);
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.println("║  Scenario              │ 100% EQ         │ 25% EQ           ║");
        System.out.println("║────────────────────────┼─────────────────┼──────────────────║");
        System.out.printf ("║  Subs registration (ms)│ %12d   │ %12d     ║%n",
                r100.regMs, r25.regMs);
        System.out.printf ("║  Publications sent     │ %12d   │ %12d     ║%n",
                r100.pubsSent, r25.pubsSent);
        System.out.printf ("║  Notifications delivered│ %12d   │ %12d     ║%n",
                r100.delivered, r25.delivered);
        System.out.printf ("║  Avg notif/publication │ %12.2f   │ %12.2f     ║%n",
                r100.matchRate(), r25.matchRate());
        System.out.printf ("║  Avg latency (ms)      │ %12.2f   │ %12.2f     ║%n",
                r100.avgLatencyMs, r25.avgLatencyMs);
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
    }

    static void writeCsv(EvalResult r100, EvalResult r25) {
        try (PrintWriter w = new PrintWriter("eval-results.csv")) {
            w.println("metric,100%_equality,25%_equality");
            w.printf ("subscriptions_registered,%d,%d%n", TOTAL_SUBS, TOTAL_SUBS);
            w.printf ("feed_seconds,%d,%d%n", FEED_SECONDS, FEED_SECONDS);
            w.printf ("registration_ms,%d,%d%n", r100.regMs, r25.regMs);
            w.printf ("publications_sent,%d,%d%n", r100.pubsSent, r25.pubsSent);
            w.printf ("notifications_delivered,%d,%d%n", r100.delivered, r25.delivered);
            w.printf ("notif_per_pub,%.4f,%.4f%n", r100.matchRate(), r25.matchRate());
            w.printf ("avg_latency_ms,%.4f,%.4f%n", r100.avgLatencyMs, r25.avgLatencyMs);
            System.out.println("→ saved eval-results.csv");
        } catch (Exception e) {
            System.err.println("CSV write failed: " + e.getMessage());
        }
    }

    record EvalResult(long pubsSent, long delivered, double avgLatencyMs, long regMs, long feedMs) {
        double matchRate() { return pubsSent == 0 ? 0 : (double) delivered / pubsSent; }
    }
}
