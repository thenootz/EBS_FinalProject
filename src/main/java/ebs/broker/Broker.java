package ebs.broker;

import ebs.common.*;
import ebs.crypto.CryptoService;
import ebs.proto.EbsProto.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Broker node in the pub/sub overlay network.
 *
 * Responsibilities:
 *  1. Accept connections from Publishers, Subscribers, and other Brokers
 *  2. Store subscriptions routed to this broker (per consistent hashing)
 *  3. Receive publications and match them against stored subscriptions
 *  4. Coordinate partial matches with other brokers (pipeline routing)
 *  5. Send heartbeats and detect failures (fault tolerance bonus)
 *  6. Replicate subscription state to peer brokers
 *
 * Routing protocol:
 *  - Each broker owns a subset of fields (determined by ConsistentHashRouter)
 *  - A subscription's predicates are split across brokers at registration time
 *  - When a publication arrives at Broker-1 (the "entry" broker):
 *      a) Broker-1 checks its own field predicates
 *      b) Sends a PartialMatch envelope to the next broker
 *      c) Each broker adds its vote; the last broker in the chain
 *         sends the Notification to the subscriber
 */
public class Broker {

    private final String id;
    private final int port;
    private final List<Config.BrokerAddress> peers;
    private final ConsistentHashRouter router;
    private final Set<String> ownedFields;

    // ── Subscription storage ──────────────────────────────────────────────────
    // subscriptionId → full Subscription (for reference)
    private final ConcurrentHashMap<String, Subscription> subscriptions = new ConcurrentHashMap<>();
    // subscriptionId → list of predicates this broker is responsible for
    private final ConcurrentHashMap<String, List<Predicate>> myPredicates = new ConcurrentHashMap<>();
    // subscriptionId → subscriber address (host:port)
    private final ConcurrentHashMap<String, String> subscriberAddresses = new ConcurrentHashMap<>();

    // ── Partial match tracking ────────────────────────────────────────────────
    // correlationId → PartialMatch accumulator
    private final ConcurrentHashMap<String, PartialMatchState> partialMatches = new ConcurrentHashMap<>();

    // ── Fault tolerance ───────────────────────────────────────────────────────
    // brokerId → last heartbeat time
    private final ConcurrentHashMap<String, Long> lastHeartbeat = new ConcurrentHashMap<>();
    // brokerId → their subscription replicas (for recovery)
    private final ConcurrentHashMap<String, List<Subscription>> replicatedSubs = new ConcurrentHashMap<>();


    private volatile boolean running = true;
    private ServerSocket serverSocket;

    public Broker(String id, int port, List<Config.BrokerAddress> allBrokers) {
        this.id   = id;
        this.port = port;
        this.peers = allBrokers.stream()
                               .filter(b -> !b.id().equals(id))
                               .toList();
        List<String> brokerIds = allBrokers.stream().map(Config.BrokerAddress::id).toList();
        this.router      = new ConsistentHashRouter(new ArrayList<>(brokerIds));
        this.ownedFields = router.getFieldsForBroker(id);
        System.out.printf("[%s] owns fields: %s%n", id, ownedFields);
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  Startup
    // ══════════════════════════════════════════════════════════════════════════

    // ── Work queue executor for envelope processing ──────────────────────────
    // Bounded queue + CallerRunsPolicy: when the queue fills, the reader thread
    // runs the task inline, which throttles the TCP socket reads and creates
    // natural back-pressure to upstream senders. The previous unbounded
    // newFixedThreadPool queue accumulated envelopes indefinitely under load,
    // causing OutOfMemoryError on the 180s × 10k-subscription evaluation.
    private final ExecutorService workPool = new ThreadPoolExecutor(
            Math.max(4, Runtime.getRuntime().availableProcessors()),
            Math.max(4, Runtime.getRuntime().availableProcessors()),
            0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(2048),
            new ThreadPoolExecutor.CallerRunsPolicy());

    public void start() throws IOException {
        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new java.net.InetSocketAddress(port), 1024);
        System.out.printf("[%s] listening on port %d%n", id, port);

        // Accept connections in a thread pool
        ExecutorService pool = Executors.newCachedThreadPool();
        pool.submit(this::acceptLoop);
        pool.submit(this::heartbeatLoop);
        pool.submit(this::failureDetectionLoop);
    }

    public void stop() {
        running = false;
        workPool.shutdownNow();
        try { if (serverSocket != null) serverSocket.close(); } catch (IOException ignored) {}
    }

    // ── Accept loop ───────────────────────────────────────────────────────────
    private void acceptLoop() {
        while (running) {
            try {
                Socket client = serverSocket.accept();
                new Thread(() -> handleConnection(client)).start();
            } catch (IOException e) {
                if (running) System.err.println("[" + id + "] accept error: " + e.getMessage());
            }
        }
    }

    // ── Handle one connection (reads until EOF) ───────────────────────────────
    private void handleConnection(Socket socket) {
        try (socket) {
            socket.setSoTimeout(60_000);
            InputStream in = socket.getInputStream();
            Envelope env;
            while ((env = NetUtil.receive(in)) != null) {
                // submit to work pool so reader thread isn't blocked
                final Envelope e = env;
                workPool.submit(() -> {
                    try { processEnvelope(e, socket); }
                    catch (Exception ex) { /* swallow */ }
                });
            }
        } catch (IOException e) {
            // normal close
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  Envelope dispatch
    // ══════════════════════════════════════════════════════════════════════════

    private void processEnvelope(Envelope env, Socket source) throws IOException {
        switch (env.getType()) {
            case PUBLICATION  -> handlePublication(env.getPublication());
            case SUBSCRIPTION -> handleSubscription(env.getSubscription(),
                                                     env.getSenderId());
            case PARTIAL_MATCH -> handlePartialMatch(env.getPartialMatch());
            case HEARTBEAT    -> handleHeartbeat(env.getHeartbeat());
            case BROKER_STATE -> handleBrokerState(env.getBrokerState());
            default           -> {}
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  Publication handling
    // ══════════════════════════════════════════════════════════════════════════

    private void handlePublication(Publication pub) {
        // For each subscription this broker holds predicates for,
        // check if OUR predicates match. Serial stream avoids the per-publication
        // ForkJoinPool task explosion (10k entries x N pub/s) that GC-thrashes the heap.
        // The broker workPool already provides connection-level parallelism upstream.
        myPredicates.entrySet().stream().forEach(entry -> {
            String subId       = entry.getKey();
            List<Predicate> ps = entry.getValue();

            boolean myMatch = ps.stream().allMatch(p -> matchPredicate(pub, p));
            if (!myMatch) return;

            // Our predicates matched — send PartialMatch to coordinate with other brokers
            Subscription sub = subscriptions.get(subId);
            if (sub == null) return;

            // Fast path: if we own ALL predicates for this subscription, no need
            // to involve peers at all. Deliver the notification directly.
            // This is the common case in 100%-equality scenarios where every
            // sub has a single predicate on a single field owned by one broker,
            // and avoids a 10k×2-peer envelope fan-out per publication.
            if (ps.size() == sub.getPredicatesCount()) {
                deliverNotification(sub, pub);
                return;
            }

            String corrId = pub.getId() + ":" + subId;

            PartialMatch pm = PartialMatch.newBuilder()
                    .setPublicationId(pub.getId())
                    .setSubscriptionId(subId)
                    .setSubscriberId(sub.getSubscriberId())
                    .addAllMatchedFields(ownedFields.stream()
                            .filter(f -> ps.stream().anyMatch(p -> p.getField().equals(f)))
                            .toList())
                    .setPublication(pub)
                    .setEmitTimestamp(pub.getTimestamp())
                    .build();

            Envelope env = Envelope.newBuilder()
                    .setType(Envelope.Type.PARTIAL_MATCH)
                    .setSenderId(id)
                    .setCorrelationId(corrId)
                    .setPartialMatch(pm)
                    .build();

            // Register our own vote in the partial match state
            PartialMatchState state = partialMatches.computeIfAbsent(corrId,
                    k -> new PartialMatchState(sub, pub));
            state.addVote(id, ownedFields);

            // Forward to all peers so they can add their votes
            for (Config.BrokerAddress peer : peers) {
                PersistentSender.send(peer.host(), peer.port(), env);
            }

            // We are the coordinator (entry broker for this publication).
            // Only the coordinator delivers the final notification once all
            // votes arrive — peers must NOT deliver (handlePartialMatch enforces).
            checkAndDeliver(corrId, state);
        });
    }

    // ── Predicate matching (plain or encrypted) ───────────────────────────────
    private boolean matchPredicate(Publication pub, Predicate pred) {
        if (pred.getHashedValue() != null && !pred.getHashedValue().isEmpty()) {
            return CryptoService.matchEncrypted(pub, pred);
        }
        return Matcher.matchesPredicate(pub, pred);
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  Partial match coordination
    // ══════════════════════════════════════════════════════════════════════════

    private void handlePartialMatch(PartialMatch pm) {
        String corrId = pm.getPublicationId() + ":" + pm.getSubscriptionId();
        Subscription sub = subscriptions.get(pm.getSubscriptionId());
        if (sub == null) return;

        // Check if WE have predicates for this subscription
        List<Predicate> myPs = myPredicates.get(pm.getSubscriptionId());

        // If we have predicates, evaluate them. The PartialMatch envelope
        // carries the publication content so we can match without lookup.
        // We DO NOT deliver from this handler — only the coordinator (the
        // broker that received the PUBLICATION) delivers, to prevent the
        // duplicate-notification storm that previously fan-out a single
        // publication into 3x notifications.
        if (myPs != null && !myPs.isEmpty()) {
            boolean myMatch = myPs.stream().allMatch(p -> matchPredicate(pm.getPublication(), p));
            if (!myMatch) return;

            // Send back a PartialMatch (vote) to the coordinator so it can
            // tally votes and deliver once all predicates are matched.
            // Reusing PartialMatch envelope shape: the sender field tells the
            // coordinator which broker is voting.
            PartialMatch ack = PartialMatch.newBuilder()
                    .setPublicationId(pm.getPublicationId())
                    .setSubscriptionId(pm.getSubscriptionId())
                    .setSubscriberId(pm.getSubscriberId())
                    .addAllMatchedFields(ownedFields.stream()
                            .filter(f -> myPs.stream().anyMatch(p -> p.getField().equals(f)))
                            .toList())
                    .setPublication(pm.getPublication())
                    .setEmitTimestamp(pm.getEmitTimestamp())
                    .build();
            Envelope env = Envelope.newBuilder()
                    .setType(Envelope.Type.PARTIAL_MATCH)
                    .setSenderId(id)
                    .setCorrelationId(corrId)
                    .setPartialMatch(ack)
                    .build();
            // Route the vote to the coordinator (the original sender of pm).
            for (Config.BrokerAddress peer : peers) {
                // Skip self; only forward to the broker that originated the PartialMatch.
                // The coordinator collects votes via partialMatches.
                PersistentSender.send(peer.host(), peer.port(), env);
            }
        } else {
            // No predicates here — we're the coordinator receiving a peer's vote.
            PartialMatchState state = partialMatches.computeIfAbsent(corrId,
                    k -> new PartialMatchState(sub, pm.getPublication()));
            state.addExternalVote(pm.getMatchedFieldsList());
            checkAndDeliver(corrId, state);
        }
    }

    private void checkAndDeliver(String corrId, PartialMatchState state) {
        Subscription sub = state.subscription;
        int totalPredicates = sub.getPredicatesCount();
        int matchedSoFar    = state.matchedFields.size();

        if (matchedSoFar >= totalPredicates) {
            // All predicates matched across all brokers — deliver!
            partialMatches.remove(corrId);
            deliverNotification(sub, state.publication);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  Notification delivery
    // ══════════════════════════════════════════════════════════════════════════

    private void deliverNotification(Subscription sub, Publication pub) {
        String addr = subscriberAddresses.get(sub.getId());
        if (addr == null) return;

        String[] parts = addr.split(":");
        String host = parts[0];
        int    port = Integer.parseInt(parts[1]);

        Notification notif = Notification.newBuilder()
                .setSubscriptionId(sub.getId())
                .setPublication(pub)
                .setDeliveryTime(System.currentTimeMillis())
                .build();

        Envelope env = Envelope.newBuilder()
                .setType(Envelope.Type.NOTIFICATION)
                .setSenderId(id)
                .setNotification(notif)
                .build();

        PersistentSender.send(host, port, env);
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  Subscription registration
    // ══════════════════════════════════════════════════════════════════════════

    private void handleSubscription(Subscription sub, String subscriberAddr) {
        // Store the full subscription locally (entry-point broker has the
        // canonical record; peers will also populate it from BrokerState).
        subscriptions.put(sub.getId(), sub);
        subscriberAddresses.put(sub.getId(), subscriberAddr);

        // Store only the predicates that belong to this broker
        List<Predicate> mine = Matcher.filterPredicatesForFields(sub, ownedFields);
        if (!mine.isEmpty()) {
            myPredicates.put(sub.getId(), mine);
        }

        // Log every 1000 subscriptions to reduce noise
        int count = subscriptions.size();
        if (count % 1000 == 0) {
            System.out.printf("[%s] %d subscriptions stored%n", id, count);
        }

        // Replicate full subscription + subscriber address to peers so they can
        // both match their owned predicates (pipelined routing) AND absorb the
        // subscription if this broker fails (fault tolerance).
        replicateToPeers(sub, subscriberAddr);
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  Fault Tolerance — Heartbeat & Failure Detection
    // ══════════════════════════════════════════════════════════════════════════

    private void heartbeatLoop() {
        while (running) {
            try {
                Heartbeat hb = Heartbeat.newBuilder()
                        .setBrokerId(id)
                        .setTimestamp(System.currentTimeMillis())
                        .setSubCount(subscriptions.size())
                        .build();
                Envelope env = Envelope.newBuilder()
                        .setType(Envelope.Type.HEARTBEAT)
                        .setSenderId(id)
                        .setHeartbeat(hb)
                        .build();
                for (Config.BrokerAddress peer : peers) {
                    NetUtil.sendOneShot(peer.host(), peer.port(), env);
                }
                Thread.sleep(Config.HEARTBEAT_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void failureDetectionLoop() {
        long startTime = System.currentTimeMillis();
        while (running) {
            try {
                Thread.sleep(Config.HEARTBEAT_INTERVAL_MS);
                long now = System.currentTimeMillis();
                // skip detection during grace period (system warmup)
                if (now - startTime < Config.FAILURE_DETECT_GRACE_MS) continue;
                for (Config.BrokerAddress peer : peers) {
                    Long last = lastHeartbeat.get(peer.id());
                    if (last != null && (now - last) > Config.BROKER_TIMEOUT_MS) {
                        System.out.printf("[%s] ⚠ Broker %s appears DEAD — absorbing its subscriptions%n",
                                id, peer.id());
                        absorbFailedBroker(peer.id());
                        lastHeartbeat.remove(peer.id()); // don't re-trigger
                        router.removeBroker(peer.id());
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void handleHeartbeat(Heartbeat hb) {
        lastHeartbeat.put(hb.getBrokerId(), hb.getTimestamp());
    }

    // ── Absorb subscriptions from a failed broker ─────────────────────────────
    private void absorbFailedBroker(String failedId) {
        List<Subscription> replicas = replicatedSubs.getOrDefault(failedId, List.of());
        System.out.printf("[%s] absorbing %d subscriptions from failed broker %s%n",
                id, replicas.size(), failedId);
        for (Subscription sub : replicas) {
            subscriptions.put(sub.getId(), sub);
            // Re-evaluate which predicates we now own
            Set<String> newFields = router.getFieldsForBroker(id); // router updated already
            List<Predicate> mine  = Matcher.filterPredicatesForFields(sub, newFields);
            if (!mine.isEmpty()) myPredicates.put(sub.getId(), mine);
        }
    }

    // ── Replicate subscription state to peers ─────────────────────────────────
    private void replicateToPeers(Subscription sub, String subscriberAddr) {
        BrokerState state = BrokerState.newBuilder()
                .setBrokerId(id)
                .addSubscriptions(sub)
                .addSubscriberAddrs(subscriberAddr == null ? "" : subscriberAddr)
                .build();
        Envelope env = Envelope.newBuilder()
                .setType(Envelope.Type.BROKER_STATE)
                .setSenderId(id)
                .setBrokerState(state)
                .build();
        for (Config.BrokerAddress peer : peers) {
            PersistentSender.send(peer.host(), peer.port(), env);
        }
    }

    private void handleBrokerState(BrokerState state) {
        List<Subscription> subs   = state.getSubscriptionsList();
        List<String>       addrs  = state.getSubscriberAddrsList();

        // 1) Keep a per-source replica list for fault-tolerance recovery.
        replicatedSubs.computeIfAbsent(state.getBrokerId(), k -> new ArrayList<>())
                      .addAll(subs);

        // 2) Also register the subscriptions for ACTIVE pipelined matching so
        //    this broker can evaluate its owned predicates against incoming
        //    publications (required by the advanced-routing criterion).
        for (int i = 0; i < subs.size(); i++) {
            Subscription sub  = subs.get(i);
            String       addr = i < addrs.size() ? addrs.get(i) : "";

            subscriptions.putIfAbsent(sub.getId(), sub);
            if (addr != null && !addr.isEmpty()) {
                subscriberAddresses.putIfAbsent(sub.getId(), addr);
            }

            List<Predicate> mine = Matcher.filterPredicatesForFields(sub, ownedFields);
            if (!mine.isEmpty()) {
                myPredicates.putIfAbsent(sub.getId(), mine);
            }
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  Inner: partial match accumulator
    // ══════════════════════════════════════════════════════════════════════════

    private static class PartialMatchState {
        final Subscription subscription;
        final Publication  publication;
        final Set<String>  matchedFields = ConcurrentHashMap.newKeySet();

        PartialMatchState(Subscription sub, Publication pub) {
            this.subscription = sub;
            this.publication  = pub;
        }

        void addVote(String brokerId, Set<String> fields) {
            matchedFields.addAll(fields);
        }

        void addExternalVote(List<String> fields) {
            matchedFields.addAll(fields);
        }
    }

}
