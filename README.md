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

Predicatele unei subscripții se distribuie pe brokeri diferiți. Publicațiile sunt trimise la toți brokerii, fiecare evaluează doar predicatele câmpurilor pe care le deține, iar potrivirile parțiale sunt agregate prin mesaje `PartialMatch` cu același `correlationId`.

### Fault Tolerance

- **Heartbeat la 2s** — fiecare broker trimite ping la peers
- **Timeout 30s** — dacă lipsește heartbeat, brokerul e marcat dead
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
├── pom.xml                              # Maven config + protobuf plugin
├── README.md                            # Acest fișier
├── docs/
│   ├── EVALUATION_REPORT.md             # Raport evaluare
│   ├── PRESENTATION.md                  # Slide-uri prezentare
│   ├── architecture-diagram.mermaid     # Diagrame arhitectură
│   └── sequence-diagram.mermaid         # Diagrame secvență
├── scripts/
│   └── run-demo.sh                      # Script pornire rapidă
├── src/main/proto/
│   └── ebs.proto                        # Schema Protocol Buffers
├── src/main/java/ebs/
│   ├── Main.java                        # Entry point demo
│   ├── EvalHarness.java                 # Evaluare 3min × 2 scenarii
│   ├── Evaluator.java                   # Wrapper evaluare
│   ├── broker/Broker.java               # Nod broker
│   ├── publisher/Publisher.java         # Nod publisher
│   ├── subscriber/Subscriber.java       # Nod subscriber
│   ├── common/
│   │   ├── Config.java                  # Configurare globală
│   │   ├── Matcher.java                 # Content-based matching
│   │   ├── ConsistentHashRouter.java    # Rutare per câmp
│   │   ├── NetUtil.java                 # Send/receive Envelope
│   │   └── PersistentSender.java        # Pool de conexiuni TCP
│   ├── crypto/CryptoService.java        # AES-GCM + SHA-256 matching
│   └── generator/
│       ├── PublicationGenerator.java    # Random publications
│       └── SubscriptionGenerator.java   # Random subscriptions (Fisher-Yates)
└── src/test/java/ebs/
    ├── MatcherTest.java                 # Teste matching
    └── ConsistentHashRouterTest.java    # Teste rutare
```

---

## Compilare

### Cu Maven (recomandat)
```bash
mvn clean package
```

### Manual (fără Maven)
```bash
# Generare cod Protobuf
protoc --java_out=src/main/java src/main/proto/ebs.proto

# Compilare
mkdir -p out
CLASSPATH="lib/protobuf-java.jar:lib/slf4j-api.jar:lib/slf4j-simple.jar"
javac -cp "$CLASSPATH" -d out $(find src/main/java -name "*.java")
```

---

## Rulare

### Demo rapid (30 secunde, 300 subscripții)
```bash
java -cp "out:lib/*" ebs.Main
```

### Demo cu encryption (bonus)
```bash
java -cp "out:lib/*" ebs.Main --encrypted
```

### Demo cu fault tolerance (bonus)
```bash
java -cp "out:lib/*" ebs.Main --fault-test
# La t=10s, broker-2 este oprit, brokerii rămași absorb subscripțiile lui
```

### Evaluare completă (3 minute × 2 scenarii)
```bash
java -Dfeed.seconds=180 -cp "out:lib/*" ebs.EvalHarness
```

Generează fișierul `eval-results.csv` cu metricile complete.

### Script all-in-one
```bash
./scripts/run-demo.sh
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
