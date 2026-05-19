package ebs.publisher;

import ebs.common.*;
import ebs.crypto.CryptoService;
import ebs.generator.PublicationGenerator;
import ebs.proto.EbsProto.*;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Publisher {

    private final String id;
    private final List<Config.BrokerAddress> brokers;
    private final PublicationGenerator generator;
    private final boolean useEncryption;
    private final CryptoService crypto;
    private final int intervalMs;

    public final AtomicLong pubsSent = new AtomicLong();
    private volatile boolean running = false;
    // Round-robin index for selecting one entry broker per publication.
    // Each publication is routed to a single entry broker; that broker
    // coordinates pipeline matching across its peers (see Broker.handlePublication).
    // Sending to all brokers would triple the inbound load and cause duplicate fan-out.
    private long pubCounter = 0;

    public Publisher(String id, List<Config.BrokerAddress> brokers,
                     boolean useEncryption, CryptoService crypto) {
        this(id, brokers, useEncryption, crypto, Config.PUBLISH_INTERVAL_MS);
    }

    public Publisher(String id, List<Config.BrokerAddress> brokers,
                     boolean useEncryption, CryptoService crypto, int intervalMs) {
        this.id            = id;
        this.brokers       = brokers;
        this.generator     = new PublicationGenerator();
        this.useEncryption = useEncryption;
        this.crypto        = crypto;
        this.intervalMs    = intervalMs;
    }

    public void startAsync() {
        running = true;
        Thread t = new Thread(this::publishLoop, id + "-thread");
        t.setDaemon(true);
        t.start();
    }

    public void stop() { running = false; }

    private void publishLoop() {
        while (running) {
            Publication pub = generator.next();
            sendToBrokers(pub);
            pubsSent.incrementAndGet();
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void sendToBrokers(Publication pub) {
        Publication toSend = useEncryption ? crypto.encryptPublication(pub) : pub;
        Envelope env = Envelope.newBuilder()
                .setType(Envelope.Type.PUBLICATION)
                .setSenderId(id)
                .setCorrelationId(pub.getId())
                .setPublication(toSend)
                .build();
        // Send to a single entry broker (round-robin). The receiving broker
        // runs pipeline matching and forwards PartialMatch envelopes to its peers.
        Config.BrokerAddress entry = brokers.get((int) (Math.floorMod(pubCounter++, brokers.size())));
        PersistentSender.send(entry.host(), entry.port(), env);
    }
}
