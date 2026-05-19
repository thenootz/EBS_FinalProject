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
        for (Config.BrokerAddress broker : brokers) {
            PersistentSender.send(broker.host(), broker.port(), env);
        }
    }
}
