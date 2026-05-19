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
        pool.submit(this::partialMatchCleanupLoop);
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
            case BATCH_PARTIAL_MATCH -> handleBatchPartialMatch(env.getBatchPartialMatch(),
                                                                env.getSenderId());
            case HEARTBEAT    -> handleHeartbeat(env.getHeartbeat());
            case BROKER_STATE -> handleBrokerState(env.getBrokerState());
            default           -> {}  // PARTIAL_MATCH (legacy single-sub) no longer used
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  Publication handling
    // ══════════════════════════════════════════════════════════════════════════

    private void handlePublication(Publication pub) {
        // ── ENTRY-BROKER LOGIC ────────────────────────────────────────────────
        // Publications arrive here ONLY from publishers (peer brokers forward
        // publications via BATCH_PARTIAL_MATCH envelopes instead).
        //
        // Strategy: evaluate own predicates locally, then forward the pub ONCE
        // to each peer via a single BATCH_PARTIAL_MATCH envelope. Peers vote
        // back with their matches in another single envelope per peer.
        //
        // Per-pub network cost: 2 forwards + ≤2 vote responses = 4 envelopes.
        // (Previous design: O(matches) envelopes per pub → 1800+ at 10k subs.)

        // 1. Local evaluation of own predicates.
        for (Map.Entry<String, List<Predicate>> entry : myPredicates.entrySet()) {
            String subId       = entry.getKey();
            List<Predicate> ps = entry.getValue();

            if (!ps.stream().allMatch(p -> matchPredicate(pub, p))) continue;

            Subscription sub = subscriptions.get(subId);
            if (sub == null) continue;

            // Fast path: we own all of this sub's predicates — deliver directly,
            // no peer coordination needed (typical for 100%-equality on owned field).
            if (ps.size() == sub.getPredicatesCount()) {
                deliverNotification(sub, pub);
                continue;
            }

            // Otherwise register a partial-match state seeded with our own
            // matched fields. Peer votes will be merged here when they arrive.
            String corrId = pub.getId() + ":" + subId;
            PartialMatchState state = partialMatches.computeIfAbsent(corrId,
                    k -> new PartialMatchState(sub, pub));
            Set<String> myFields = ownedFields.stream()
                    .filter(f -> ps.stream().anyMatch(p -> p.getField().equals(f)))
                    .collect(java.util.stream.Collectors.toSet());
            state.matchedFields.addAll(myFields);
            // Cannot deliver yet — sub has predicates on other brokers; wait for votes.
        }

        // 2. Forward the publication to every peer for their independent match
        //    pass. Empty matches list signals "this is a forward, please evaluate
        //    and vote back".
        if (peers.isEmpty()) return;
        BatchPartialMatch forward = BatchPartialMatch.newBuilder()
                .setPublication(pub)
                .setEmitTimestamp(pub.getTimestamp())
                .setIsResponse(false)
                .build();
        Envelope env = Envelope.newBuilder()
                .setType(Envelope.Type.BATCH_PARTIAL_MATCH)
                .setSenderId(id)
                .setBatchPartialMatch(forward)
                .build();
        for (Config.BrokerAddress peer : peers) {
            PersistentSender.send(peer.host(), peer.port(), env);
        }
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

    // ── Batched broker-to-broker partial match ────────────────────────────────
    // is_response==false : forward from coordinator to peer (publication only)
    // is_response==true  : peer's vote back to coordinator (publication + matches)
    private void handleBatchPartialMatch(BatchPartialMatch batch, String senderId) {
        if (!batch.getIsResponse()) {
            // ── PEER ROLE: evaluate own predicates against the forwarded pub. ──
            Publication pub = batch.getPublication();
            List<MatchedSub> votes = new ArrayList<>();
            for (Map.Entry<String, List<Predicate>> entry : myPredicates.entrySet()) {
                String subId       = entry.getKey();
                List<Predicate> ps = entry.getValue();
                if (!ps.stream().allMatch(p -> matchPredicate(pub, p))) continue;

                List<String> myFields = ownedFields.stream()
                        .filter(f -> ps.stream().anyMatch(p -> p.getField().equals(f)))
                        .toList();
                votes.add(MatchedSub.newBuilder()
                        .setSubscriptionId(subId)
                        .addAllMatchedFields(myFields)
                        .build());
            }

            // Always respond, even when votes is empty, so the coordinator can
            // make progress and (eventually) prune state.
            BatchPartialMatch response = BatchPartialMatch.newBuilder()
                    .setPublication(pub)
                    .setEmitTimestamp(batch.getEmitTimestamp())
                    .setIsResponse(true)
                    .addAllMatches(votes)
                    .build();
            Envelope env = Envelope.newBuilder()
                    .setType(Envelope.Type.BATCH_PARTIAL_MATCH)
                    .setSenderId(id)
                    .setBatchPartialMatch(response)
                    .build();
            Config.BrokerAddress coord = peerById(senderId);
            if (coord != null) {
                PersistentSender.send(coord.host(), coord.port(), env);
            }
            return;
        }

        // ── COORDINATOR ROLE: merge peer votes into per-(pub,sub) state. ──
        Publication pub = batch.getPublication();
        for (MatchedSub ms : batch.getMatchesList()) {
            Subscription sub = subscriptions.get(ms.getSubscriptionId());
            if (sub == null) continue;
            String corrId = pub.getId() + ":" + ms.getSubscriptionId();
            PartialMatchState state = partialMatches.computeIfAbsent(corrId,
                    k -> new PartialMatchState(sub, pub));
            state.matchedFields.addAll(ms.getMatchedFieldsList());
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

    // Lookup a peer broker by id; returns null if not a peer.
    private Config.BrokerAddress peerById(String brokerId) {
        for (Config.BrokerAddress p : peers) {
            if (p.id().equals(brokerId)) return p;
        }
        return null;
    }

    // Evicts partial-match state older than 30s. Bounds memory in adversarial
    // scenarios (e.g., a sub with predicates that partially match but never
    // accumulate enough votes for delivery, or a peer broker that crashes
    // mid-flow without sending its vote).
    private void partialMatchCleanupLoop() {
        while (running) {
            try {
                Thread.sleep(5_000);
                long cutoff = System.currentTimeMillis() - 30_000;
                partialMatches.entrySet().removeIf(e -> e.getValue().createdAt < cutoff);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
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
        final long         createdAt = System.currentTimeMillis();

        PartialMatchState(Subscription sub, Publication pub) {
            this.subscription = sub;
            this.publication  = pub;
        }
    }

}
