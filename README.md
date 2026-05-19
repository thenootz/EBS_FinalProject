# EBS Pub/Sub System

Implementare a unui sistem **publish/subscribe content-based** distribuit, dezvoltat ca proiect pentru cursul **Sisteme Bazate pe Evenimente** (UAIC Iași).

---

## Componente

| Componentă | Rol |
|---|---|
| **3 Brokers** | Rețea overlay; stochează subscripții și fac matching content-based |
| **2 Publishers** | Generează publicații cu date random și le transmit prin Protobuf |
| **3 Subscribers** | Înregistrează subscripții și primesc notificări la match |

---

## Cerințe acoperite

| # | Cerință | Punctaj | Status |
|---|---|---|---|
| 1 | Flux de publicații (2 publishers) | 5p | ✅ |
| 2 | Overlay 3 brokeri cu matching | 10p | ✅ |
| 3 | 3 subscribers cu subscripții distribuite | 5p | ✅ |
| 4 | Rutare avansată (consistent hashing + pipeline) | 5p | ✅ |
| 5 | Evaluare 10k subs × 3min × 2 scenarii | 5p | ✅ |
| **B1** | **Protocol Buffers (serializare binară)** | **5p** | ✅ |
| **B2** | **Fault tolerance (heartbeat + replicare)** | **5p** | ✅ |
| **B3** | **Filtrare criptată (AES-GCM + SHA-256)** | **5-10p** | ✅ |

---

## Arhitectură

```
┌──────────────┐ Protobuf  ┌─────────────────────────────────┐
│ Publisher 1  │──────────►│                                 │
├──────────────┤           │     Broker overlay (3 nodes)    │
│ Publisher 2  │──────────►│                                 │
└──────────────┘           │   ┌────────┐                    │
                           │   │broker-1│◄──┐                │
                           │   └────┬───┘   │ heartbeat      │
                           │        │       │ + replicare    │
                           │   ┌────▼───┐   │                │
                           │   │broker-2│───┤                │
                           │   └────┬───┘   │                │
                           │        │       │                │
                           │   ┌────▼───┐   │                │
                           │   │broker-3│───┘                │
                           │   └────┬───┘                    │
                           └────────┼────────────────────────┘
                                    │ notifications
                  ┌─────────────────┼─────────────────┐
                  ▼                 ▼                 ▼
            ┌──────────┐      ┌──────────┐      ┌──────────┐
            │ Subscriber│      │ Subscriber│      │ Subscriber│
            │   sub-1  │      │   sub-2  │      │   sub-3  │
            └──────────┘      └──────────┘      └──────────┘
```

### Rutare avansată — Consistent Hashing

Fiecare câmp este atribuit determinist unui broker prin **SHA-256 % numBrokers**:

```
hash("company")   % 3 → broker-3   (owns: company, drop)
hash("value")     % 3 → broker-1   (owns: value)
hash("drop")      % 3 → broker-3
hash("variation") % 3 → broker-2   (owns: variation, date)
hash("date")      % 3 → broker-2
```

Predicatele unei subscripții se distribuie pe brokeri diferiți. Fiecare publicație merge la **un singur broker entry** (rotativ, round-robin), care evaluează predicatele câmpurilor proprii și apoi o transmite **într-o singură envelopă `BatchPartialMatch` per peer**. Fiecare peer evaluează predicatele câmpurilor sale și votează înapoi la coordinator cu lista de subscripții potrivite. Coordinatorul agregă voturile per (publicație, subscripție) și emite notificarea când toate predicatele sunt acoperite. Per publicație se transmit doar **4 envelope inter-broker** (2 forward + 2 vote-uri) indiferent de câte subscripții se potrivesc — vezi raportul de evaluare pentru detalii.

### Fault Tolerance

- **Heartbeat la 2 s** — fiecare broker trimite ping la peers
- **Timeout 60 s** — dacă lipsește heartbeat (cu grace period de 30 s la pornire), brokerul e marcat dead
- **Replicare** — fiecare broker păstrează o copie a subscripțiilor altora în `replicatedSubs`
- **Recovery** — brokerii activi absorb subscripțiile celui căzut

### Filtrare criptată

- Câmpurile sunt **hash-uite cu SHA-256** + salt comun
- Brokerul compară hash-uri (nu vede plaintext)
- Conținutul publicației e **AES-GCM encrypted**
- Subscriberul decriptează local cu cheia partajată
- Limitare: operatorii `<`, `>`, `<=`, `>=` nu sunt compatibili cu hash-ing

---

## Structura proiectului

```
pubsub/
├── pom.xml                              # Maven config + protobuf plugin (shaded jar)
├── README.md                            # Acest fișier
├── PROJECT_KNOWLEDGE.md                 # Sursa de adevăr pentru continuarea proiectului
├── docs/
│   ├── EVALUATION_REPORT.md             # Raport evaluare
│   ├── PRESENTATION.md                  # Slide-uri prezentare
│   ├── architecture-diagram.mermaid     # Diagrame arhitectură
│   ├── sequence-diagram.mermaid         # Diagrame secvență
│   └── fault-tolerance-diagram.mermaid
├── scripts/
│   └── run-demo.sh                      # Script pornire rapidă
├── src/main/proto/
│   └── ebs.proto                        # Schema Protocol Buffers
├── src/main/java/ebs/
│   ├── Main.java                        # Entry point demo
│   ├── EvalHarness.java                 # Evaluare 3 min × 2 scenarii
│   ├── broker/Broker.java               # Nod broker (matching + pipeline + fault tolerance)
│   ├── publisher/Publisher.java         # Nod publisher (round-robin la un broker entry)
│   ├── subscriber/Subscriber.java       # Nod subscriber
│   ├── common/
│   │   ├── Config.java                  # Configurare globală
│   │   ├── Matcher.java                 # Content-based matching
│   │   ├── ConsistentHashRouter.java    # Rutare per câmp
│   │   ├── NetUtil.java                 # Send/receive Envelope (length-prefixed)
│   │   └── PersistentSender.java        # Pool de conexiuni TCP persistente
│   ├── crypto/CryptoService.java        # AES-GCM + SHA-256 matching
│   └── generator/
│       ├── PublicationGenerator.java    # Random publications
│       └── SubscriptionGenerator.java   # Random subscriptions (Fisher-Yates)
└── src/test/java/ebs/
    ├── MatcherTest.java                 # Teste matching (executabile ca `java -cp ...`)
    ├── ConsistentHashRouterTest.java
    ├── SubscriptionGeneratorTest.java
    └── CryptoServiceTest.java
```

---

## Compilare

### Cu Maven (singura cale suportată)
```bash
mvn clean package
```

Produce JAR-ul shaded `target/pubsub-1.0.jar` (cu Protobuf + SLF4J incluse). Dependențele se descarcă automat din Maven Central — proiectul nu mai folosește un folder `lib/` cu JAR-uri precompilate.

---

## Rulare

### Demo rapid (30 secunde, 300 subscripții)
```bash
java -cp target/pubsub-1.0.jar ebs.Main
```

### Demo cu encryption (bonus)
```bash
java -cp target/pubsub-1.0.jar ebs.Main --encrypted
```

### Demo cu fault tolerance (bonus)
```bash
java -cp target/pubsub-1.0.jar ebs.Main --fault-test
# La t=10s, broker-2 este oprit, brokerii rămași absorb subscripțiile lui
```

### Evaluare completă (3 minute × 2 scenarii)
```bash
java -Xmx2g -Dfeed.seconds=180 -Dtotal.subs=10000 -cp target/pubsub-1.0.jar ebs.EvalHarness
```

Generează fișierul `eval-results.csv` cu metricile complete (vezi `docs/EVALUATION_REPORT.md`).

### Script all-in-one
```bash
./scripts/run-demo.sh demo         # Demo 30s
./scripts/run-demo.sh encrypted    # Cu encryption
./scripts/run-demo.sh fault        # Cu broker failure
./scripts/run-demo.sh eval 180     # Evaluare 3 min × 2 scenarii
./scripts/run-demo.sh clean        # Curăță artefacte
```

---

## Porturi folosite

| Componentă | Port |
|---|---|
| broker-1 | 5001 |
| broker-2 | 5002 |
| broker-3 | 5003 |
| sub-1 | 7001 |
| sub-2 | 7002 |
| sub-3 | 7003 |

Asigură-te că aceste porturi sunt libere înainte de rulare:
```bash
fuser -k 5001/tcp 5002/tcp 5003/tcp 7001/tcp 7002/tcp 7003/tcp
```

---

## Echipa

Proiect realizat în echipă de **2 studenți** pentru cursul SBE, UAIC Iași.

## Tehnologii

- **Java 17**
- **Maven 3.x**
- **Google Protocol Buffers 3.21**
- **AES-GCM** (Java Cryptography API)
- **SHA-256** (Java MessageDigest)
- **TCP Sockets** (java.net)
