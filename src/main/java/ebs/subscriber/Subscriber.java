package ebs.subscriber;

import ebs.common.*;
import ebs.crypto.CryptoService;
import ebs.generator.SubscriptionGenerator;
import ebs.proto.EbsProto.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Subscriber node.
 *
 * - Connects randomly to brokers to register subscriptions
 *   (subscriptions are distributed across multiple brokers via consistent hashing)
 * - Listens on a local port for incoming Notification messages
 * - Tracks delivery latency and match count for evaluation
 */
public class Subscriber {

    private final String id;
    private final int    listenPort;
    private final List<Config.BrokerAddress> brokers;
    private final CryptoService crypto;
    private final boolean useEncryption;
    private final Random rng = new Random();

    // ── Stats ─────────────────────────────────────────────────────────────────
    public final AtomicLong  notificationsReceived = new AtomicLong();
    public final AtomicLong  totalLatencyMs        = new AtomicLong();


    private volatile boolean running = true;
    private ServerSocket serverSocket;

    public Subscriber(String id, int listenPort,
                      List<Config.BrokerAddress> brokers,
                      CryptoService crypto, boolean useEncryption) {
        this.id            = id;
        this.listenPort    = listenPort;
        this.brokers       = brokers;
        this.crypto        = crypto;
        this.useEncryption = useEncryption;
    }

    /** Start listening for notifications. */
    public void startListening() throws IOException {
        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new java.net.InetSocketAddress(listenPort));
        Thread t = new Thread(this::acceptLoop, id + "-listener");
        t.setDaemon(true);
        t.start();
        System.out.printf("[%s] listening for notifications on port %d%n", id, listenPort);
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket client = serverSocket.accept();
                new Thread(() -> handleNotification(client)).start();
            } catch (IOException e) {
                if (running) System.err.println("[" + id + "] accept error: " + e.getMessage());
            }
        }
    }

    private void handleNotification(Socket socket) {
        try (socket) {
            Envelope env = NetUtil.receive(socket.getInputStream());
            if (env == null || env.getType() != Envelope.Type.NOTIFICATION) return;

            Notification notif = env.getNotification();
            Publication  pub   = notif.getPublication();

            // Decrypt if needed
            if (useEncryption && pub.getIsEncrypted()) {
                pub = crypto.decryptPublication(pub);
            }

            long latency = notif.getDeliveryTime() - pub.getTimestamp();
            notificationsReceived.incrementAndGet();
            totalLatencyMs.addAndGet(Math.max(0, latency));

        } catch (IOException e) {
            System.err.println("[" + id + "] notification error: " + e.getMessage());
        }
    }

    // ── Register subscriptions ────────────────────────────────────────────────

    /**
     * Register `count` subscriptions, distributing them randomly across brokers.
     * Each subscription's predicates are routed to the appropriate broker
     * via the ConsistentHashRouter on the broker side.
     */
    public void registerSubscriptions(int count, SubscriptionGenerator gen) {
        ConsistentHashRouter router = new ConsistentHashRouter(
                brokers.stream().map(Config.BrokerAddress::id).toList());

        for (int i = 0; i < count; i++) {
            Subscription sub = gen.next(id);

            // Encrypt predicates if needed
            Subscription toSend = sub;
            if (useEncryption) {
                Subscription.Builder enc = sub.toBuilder().clearPredicates();
                sub.getPredicatesList().forEach(p -> enc.addPredicates(crypto.encryptPredicate(p)));
                toSend = enc.build();
            }

            // Pick a random broker as entry point — broker will forward to peers as needed
            Config.BrokerAddress target = brokers.get(rng.nextInt(brokers.size()));

            // Subscriber address so broker knows where to deliver notifications
            String myAddr = "localhost:" + listenPort;

            Envelope env = Envelope.newBuilder()
                    .setType(Envelope.Type.SUBSCRIPTION)
                    .setSenderId(myAddr)      // reuse senderId as "return address"
                    .setSubscription(toSend)
                    .build();

            PersistentSender.send(target.host(), target.port(), env);
        }
        System.out.printf("[%s] registered %d subscriptions%n", id, count);
    }

    public void stop() {
        running = false;
        try { if (serverSocket != null) serverSocket.close(); } catch (IOException ignored) {}
    }

    // ── Stats accessors ───────────────────────────────────────────────────────
    public double getAverageLatencyMs() {
        long count = notificationsReceived.get();
        return count == 0 ? 0.0 : (double) totalLatencyMs.get() / count;
    }
}
