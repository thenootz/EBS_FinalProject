# PROJECT_KNOWLEDGE.md — EBS Pub/Sub System

> **Scop:** Acest document este sursa de adevăr pentru continuarea proiectului în sesiuni viitoare cu Claude. Atașează-l ca knowledge într-un Project pe claude.ai sau încarcă-l într-o conversație nouă pentru a restaura instant tot contextul.

---

## 0. Quick-start pentru sesiuni viitoare

**Pentru a continua proiectul, spune:**
> "Citește PROJECT_KNOWLEDGE.md și hai să continuăm proiectul EBS Pub/Sub."

Apoi Claude va ști:
- Ce a fost implementat
- Ce mai trebuie făcut
- Cum să compileze și să ruleze
- Toate deciziile de design

---

## 1. Context proiect

### Curs & echipă
- **Curs:** Sisteme Bazate pe Evenimente (SBE)
- **Universitate:** Alexandru Ioan Cuza, Iași
- **Echipă:** 2 studenți
- **Strategie bonus:** Toate cele 3 bonusuri implementate (max ~45p total)

### Cerința completă
Implementare arhitectură pub/sub content-based cu:
- 1-2 publishers care emit publicații
- 2-3 brokeri în overlay
- 2-3 subscribers
- Rutare avansată: subscripțiile aceluiași subscriber distribuite balansat pe brokeri, publicațiile trec prin pipeline (nu un singur broker care face tot match-ul)
- Evaluare: 10 000 subscripții × 3 minute feed × 2 scenarii (100% și 25% egalitate pe `company`)

### Constrângeri
- **Limbaj:** Java (compatibil cu tema practică a generatorului)
- **Kafka:** Dacă s-ar folosi, doar pentru transport (NU pentru matching/storage). Noi NU folosim Kafka.
- **Transport:** TCP sockets proprii (PersistentSender pool)
- **Serializare:** Protocol Buffers (bonus 5p)

---

## 2. Decizii arhitecturale

### Stack tehnic
| Layer | Tehnologie | Motiv |
|---|---|---|
| Limbaj | Java 17 | Cerință + compatibilitate tema practică |
| Build | Maven | Standard ecosistem Java |
| Transport | TCP Sockets (length-prefixed) | Control direct, fără Kafka |
| Serializare | Protocol Buffers 3.21 | Bonus 1: binary, compact, fast |
| Crypto | AES-GCM 256 + SHA-256 | Bonus 3: encrypted matching |
| Concurrency | ExecutorService (bounded ThreadPoolExecutor + CallerRunsPolicy) | Throughput controlat la 10k subscripții, fără OOM |

### Mecanism rutare avansat: Consistent Hashing per câmp

```
hash("company")   % 3 → broker-3   (owns: company, drop)
hash("value")     % 3 → broker-1   (owns: value)
hash("drop")      % 3 → broker-3
hash("variation") % 3 → broker-2   (owns: variation, date)
hash("date")      % 3 → broker-2
```

**Cum funcționează (implementat în cod):**
1. Subscripția ajunge la **un** broker entry-point (ales aleator de subscriber).
2. Entry-point-ul stochează predicatele câmpurilor proprii în `myPredicates`.
3. Trimite la peers un `BrokerState{subscription, subscriber_addr}` (replicare).
4. Fiecare peer, la primirea `BrokerState`:
   - păstrează replica în `replicatedSubs` pentru recovery (fault tolerance),
   - **și** extrage predicatele câmpurilor proprii și le adaugă în `myPredicates` pentru matching activ (pipelined routing — cerința #4).

**La match (algoritm batched, post-refactor):**
1. Publicația ajunge la **un singur broker entry** (ales round-robin de publisher).
2. Brokerul entry evaluează serial predicatele câmpurilor proprii (`stream()` peste `myPredicates`):
   - **Fast path:** dacă brokerul deține TOATE predicatele subscripției, livrează direct, fără implicarea peers.
   - Altfel: înregistrează voturile proprii în `partialMatches[pubId:subId]` ca `PartialMatchState`.
3. Entry-ul trimite **o singură envelopă `BatchPartialMatch(publication, is_response=false)` per peer**.
4. Fiecare peer evaluează propriile predicate, construiește o listă de `MatchedSub(sub_id, matched_fields)` și își trimite votul înapoi la entry într-o singură envelopă `BatchPartialMatch(is_response=true)`.
5. Entry-ul agregă voturile în `PartialMatchState.matchedFields`; când setul acoperă toate predicatele subscripției, livrează `Notification` direct la subscriber prin `PersistentSender`.
6. Un thread `partialMatchCleanupLoop` evictă intrări mai vechi de 30 s (TTL) pentru a evita acumularea de stări orfane.

**Volum rețea per publicație:** 4 envelope inter-broker (2 forward + 2 vote-uri), indiferent de numărul de subscripții potrivite. Înainte de batching erau O(matches) per publicație (~1 800 la 10 k subs) — vezi `docs/EVALUATION_REPORT.md` §6 pentru istoric.

### Bonus 1: Protocol Buffers
- Schema în `src/main/proto/ebs.proto`
- Toate mesajele wrap-uite în `Envelope` cu enum Type + oneof payload
- ~5-10× mai compact ca JSON

### Bonus 2: Fault Tolerance
- **Heartbeat** la 2s între brokeri (`sendOneShot` — bypass coadă)
- **Grace period** 30s la pornire înainte de a porni detecția de failure
- **Timeout** 60s fără heartbeat → broker considerat DEAD
- **Replicare** — fiecare broker păstrează `replicatedSubs[failedBrokerId]` cu subscripțiile altora (carry subscriber address via `BrokerState.subscriber_addrs`)
- **Recovery** — la failure, brokerii rămași absorb subscripțiile celui căzut din replicas și re-evaluează predicatele pe care le dețin acum

### Bonus 3: Encrypted Matching
- Hash SHA-256 + salt comun pe field values
- Payload publicație criptat cu AES-GCM (256-bit)
- Broker compară hash-uri, nu vede plaintext
- **Limitare:** doar `=` și `!=` funcționează pe hash; `<`, `>` fall back la plaintext

---

## 3. Structura proiectului

```
pubsub/
├── .gitignore                            # ignoră target/, out/, EbsProto generat, IDE, etc.
├── README.md                             # Overview general
├── PROJECT_KNOWLEDGE.md                  # ← acest fișier
├── pom.xml                               # Maven config + protobuf plugin (shaded jar)
├── docs/
│   ├── EVALUATION_REPORT.md              # Raport evaluare populat (10k × 180 s)
│   ├── PRESENTATION.md                   # 14 slide-uri prezentare
│   ├── architecture-diagram.mermaid
│   ├── sequence-diagram.mermaid
│   └── fault-tolerance-diagram.mermaid
├── scripts/
│   └── run-demo.sh                       # Runner cu 6 moduri
├── src/main/proto/
│   └── ebs.proto                         # Schema Protobuf (Envelope, BatchPartialMatch, etc.)
├── src/main/java/ebs/
│   ├── Main.java                         # Entry point demo (30s)
│   ├── EvalHarness.java                  # Evaluare 3min × 2 scenarii
│   ├── broker/Broker.java                # Nod broker (matching, fault tolerance, batched pipeline)
│   ├── publisher/Publisher.java          # Nod publisher (round-robin la un broker entry)
│   ├── subscriber/Subscriber.java        # Nod subscriber
│   ├── common/
│   │   ├── Config.java                   # Porturi, timing, intervale
│   │   ├── Matcher.java                  # Content-based matching
│   │   ├── ConsistentHashRouter.java     # Rutare hash → broker
│   │   ├── NetUtil.java                  # Send/receive Envelope (TCP, length-prefixed)
│   │   └── PersistentSender.java         # Pool conexiuni TCP persistente
│   ├── crypto/CryptoService.java         # AES-GCM + SHA-256
│   ├── generator/
│   │   ├── PublicationGenerator.java     # Random publication objects
│   │   └── SubscriptionGenerator.java    # Random subs cu Fisher-Yates
│   └── proto/EbsProto.java               # AUTO-GENERAT din ebs.proto (ignorat în git)
└── src/test/java/ebs/
    ├── MatcherTest.java                  # smoke tests (vezi nota Surefire mai jos)
    ├── ConsistentHashRouterTest.java
    ├── SubscriptionGeneratorTest.java
    └── CryptoServiceTest.java
```

> **Notă dependențe:** Proiectul folosește exclusiv Maven; nu mai există folder `lib/` cu JAR-uri precompilate. `mvn package` produce `target/pubsub-1.0.jar` shaded cu Protobuf + SLF4J incluse.

> **Notă teste:** Clasele de test sunt prezente cu `public static void main(String[] args)` (smoke tests rulabile cu `java -cp target/pubsub-1.0.jar ebs.<Test>`). Configurarea curentă a `maven-surefire-plugin` raportează `Tests run: 0` — nu sunt recunoscute ca JUnit standard. De rezolvat: adăugat dependență JUnit 5 + provider Surefire compatibil.

---

## 4. Clase cheie — semnături și roluri

### `Config.java` (common)
```java
public static final int  BROKER1_PORT = 5001, BROKER2_PORT = 5002, BROKER3_PORT = 5003;
public static final int  SUBSCRIBER1_PORT = 7001, SUBSCRIBER2_PORT = 7002, SUBSCRIBER3_PORT = 7003;
public static final int  PUBLISH_INTERVAL_MS    = 200;
public static final int  HEARTBEAT_INTERVAL_MS  = 2_000;
public static final int  BROKER_TIMEOUT_MS      = 60_000;  // tolerant la încărcare
public static final int  FAILURE_DETECT_GRACE_MS = 30_000; // warmup înainte de detecție
public static final int  EVAL_SUBSCRIPTIONS     = 10_000;
public static final long EVAL_FEED_DURATION_MS  = 3 * 60 * 1_000L;
```

### `Broker.java`
- `start()` — pornește server socket (backlog 1024) + 4 thread pools (accept, heartbeat, failureDetection, partialMatchCleanup)
- `handlePublication(pub)` — entry-broker logic: serial `stream()` peste `myPredicates`, fast-path delivery când brokerul deține toate predicatele, altfel forward `BatchPartialMatch(is_response=false)` la fiecare peer
- `handleBatchPartialMatch(batch, senderId)` — dispatcher dual:
  - `is_response=false`: rol PEER — evaluează propriile predicate și trimite înapoi vot batched
  - `is_response=true`: rol COORDINATOR — agregă voturi din `MatchedSub`, livrează când toate predicatele subscripției sunt acoperite
- `handleSubscription(sub, addr)` — extrage predicatele câmpurilor proprii; replică la peers via `replicateToPeers(sub, addr)` (include adresa subscriber-ului)
- `handleBrokerState(state)` — păstrează replica în `replicatedSubs` **și** înregistrează subscripția pentru matching activ (pipelined routing — cerința #4)
- `partialMatchCleanupLoop()` — evictă intrări din `partialMatches` mai vechi de 30 s
- `absorbFailedBroker(brokerId)` — preia subscripțiile replicate de la peer-ul căzut
- `workPool` — `ThreadPoolExecutor` cu `ArrayBlockingQueue(2048)` și `CallerRunsPolicy` (backpressure naturală, previne OOM-ul observat cu `Executors.newFixedThreadPool()`)

### `Publisher.java`
- `startAsync()` — daemon thread care emite la `PUBLISH_INTERVAL_MS`
- Trimite **la un singur broker entry**, ales round-robin via `pubCounter % brokers.size()`. Brokerul entry coordonează pipeline-ul batched cu peers (vezi `Broker.handlePublication`). Fan-out-ul anterior la toți brokerii triplica volumul de intrare și livra duplicate.
- Folosește `PersistentSender` pentru reuse de socket

### `Subscriber.java`
- `startListening()` — server socket pe portul propriu
- `registerSubscriptions(N, gen)` — folosește un socket persistent per broker pentru viteză
- Track de latență: `notif.deliveryTime - pub.timestamp`

### `ConsistentHashRouter.java`
- `getBrokerForField(field) → String` — SHA-256 % brokers.size()
- `getFieldsForBroker(brokerId) → Set<String>` — invers
- `removeBroker(id)` — pentru fault tolerance (la detecția unui peer DEAD, brokerii rămași absorb subscripțiile lui)

### `Matcher.java`
- `matches(pub, sub) → boolean` — toate predicatele
- `matchesPredicate(pub, pred) → boolean` — un singur predicat
- Operatori suportați: `=`, `!=`, `<`, `<=`, `>`, `>=`

### `CryptoService.java`
- `encryptPublication(plain) → Publication` — AES-GCM cu IV random
- `decryptPublication(enc) → Publication`
- `hashValue(string) → byte[]` — SHA-256 + salt (deterministic)
- `matchEncrypted(pub, pred) → boolean` — compară hash-uri

### `SubscriptionGenerator.java`
**Strategie deterministică:** Pre-calculează exact câte subscripții conțin fiecare câmp (`ceil(freq × N)`), folosește **Fisher-Yates shuffle** pentru distribuție aleatoare a sloturilor. Garantează frecvențe **exacte** (verificat în teste).

Factory-uri:
- `allEquality(N)` — 100% `=` pe company
- `quarterEquality(N)` — 25% `=` pe company

### `EvalHarness.java`
- Rulează **2 scenarii** back-to-back
- `-Dfeed.seconds=N` configurabil (default 180)
- Generează `eval-results.csv` cu metrici
- Afișează tabel comparativ

---

## 5. Build & rulare

### Cerințe sistem
```bash
sudo apt install default-jdk-headless protobuf-compiler maven
# Sau pe macOS:
brew install openjdk@17 protobuf maven
```

Proiectul folosește **exclusiv Maven** pentru dependențe (Protobuf 3.25.1, SLF4J). Nu există folder `lib/` cu JAR-uri precompilate.

### Compilare (singura cale suportată)
```bash
mvn clean package
# Produce target/pubsub-1.0.jar (shaded, ~2 MB, include Protobuf + SLF4J)
```

### Rulare prin script
```bash
./scripts/run-demo.sh demo                 # Demo 30s
./scripts/run-demo.sh encrypted            # Cu encryption
./scripts/run-demo.sh fault                # Cu broker failure
./scripts/run-demo.sh eval 180             # Evaluare 3min × 2 scenarii
./scripts/run-demo.sh test                 # Smoke tests (vezi nota Surefire)
./scripts/run-demo.sh clean                # Curăță artefacte
```

### Rulare manuală
```bash
JAR=target/pubsub-1.0.jar

# Demo
java -cp $JAR ebs.Main
java -cp $JAR ebs.Main --encrypted
java -cp $JAR ebs.Main --fault-test

# Evaluare
java -Xmx2g -Dfeed.seconds=180 -Dtotal.subs=10000 -cp $JAR ebs.EvalHarness

# Smoke tests (clase cu public static void main)
java -cp $JAR ebs.MatcherTest
java -cp $JAR ebs.ConsistentHashRouterTest
java -cp $JAR ebs.SubscriptionGeneratorTest
java -cp $JAR ebs.CryptoServiceTest
```

### Curăță porturi blocate
```bash
fuser -k 5001/tcp 5002/tcp 5003/tcp 7001/tcp 7002/tcp 7003/tcp
```

---

## 6. Optimizări de performanță aplicate

1. **Batched PartialMatch** — o singură envelopă `BatchPartialMatch` per peer per publicație în loc de O(matches) envelope; per publicație 4 envelope inter-broker (2 forward + 2 vote-uri), indiferent de numărul de subscripții potrivite
2. **PersistentSender** — pool de socket-uri TCP per (host:port), evită handshake repetat
3. **Bounded work queue + CallerRunsPolicy** (`ArrayBlockingQueue(2048)`) — backpressure naturală, previne OOM la 10k subs
4. **Round-robin publication entry** — fiecare publicație merge la UN singur broker entry; brokerul coordonează pipeline-ul cu peers
5. **TCP_NODELAY + BufferedOutputStream** — echilibru între latență mică și throughput
6. **Heartbeat one-shot** — bypass la coada de mesaje, separat de canalul principal
7. **Subscriber stream loop** — conexiunile primite de la brokeri sunt citite în buclă (`while NetUtil.receive(...) != null`) cu `setSoTimeout(60s)`, evitând storm-ul de reconnect-uri
8. **ServerSocket backlog 1024** — înlocuiește default-ul de 50, previne saturările la înregistrarea bulk
9. **Subscribers folosesc 1 socket persistent / broker** la registration (rapid pentru 10k subs)
10. **Fast-path delivery** — când brokerul entry deține toate predicatele unei subscripții, livrează direct, fără implicarea peers
11. **Partial-match cleanup loop** — evictă intrări orfane mai vechi de 30 s
12. **BROKER_TIMEOUT_MS = 60 s + grace 30 s** — evită fals-pozitive în registration storm

### Probleme rezolvate pe parcurs
- ❌ Initial: registrare 10k subs cu socket-per-call → 30s+ (rezolvat: PersistentSender)
- ❌ Initial: latență ~10s la 10k subs (rezolvat: bounded queue + batched pipeline)
- ❌ Initial: brokerii detectau fals-pozitive (timeout=6s prea agresiv) → (crescut la 60s + grace 30s)
- ❌ Initial: heartbeats blocate pe socket-uri congestionate → (mutate la sendOneShot)
- ❌ Initial: `handleBrokerState` doar replicare-pentru-failover → fără pipeline matching la peers; cerința #4 NU era îndeplinită. **Rezolvat:** peers înregistrează predicatele proprii din replicas; `BrokerState` poartă și `subscriber_addrs` ca să poată livra `Notification` direct.
- ❌ Initial: `parallelStream` pe `myPredicates` → ForkJoinPool task explosion la 10k subs (rezolvat: serial `stream()`)
- ❌ Initial: `Executors.newFixedThreadPool()` cu coadă nemărginită → OOM la 4 min în evaluare (rezolvat: ThreadPoolExecutor cu ArrayBlockingQueue + CallerRunsPolicy)
- ❌ Initial: publisher fan-out la toți brokerii → 3x volumul de intrare + 3x livrări duplicate (rezolvat: round-robin la un singur entry broker)
- ❌ Initial: subscriber închidea socket-ul după fiecare notificare → storm de reconnect-uri (rezolvat: buclă cu `setSoTimeout`)
- ❌ Initial: `PartialMatch` per (sub, peer) → ~1 800 envelope per publicație la 10k subs, blocant pe evaluarea completă (rezolvat: `BatchPartialMatch` batched)

---

## 7. Status cerințe

| # | Cerință | Punctaj | Status |
|---|---|---|---|
| 1 | Flux publicații (2 publishers) | 5p | ✅ |
| 2 | Overlay 3 brokeri cu matching | 10p | ✅ |
| 3 | 3 subscribers cu subscripții | 5p | ✅ |
| 4 | Rutare avansată (consistent hashing + pipeline true multi-broker matching) | 5p | ✅ (peers înregistrează predicatele din `BrokerState` și votează batched) |
| 5 | Evaluare 10k subs × 3min × 2 scenarii | 5p | ✅ (rezultate în `docs/EVALUATION_REPORT.md`) |
| **B1** | **Protocol Buffers** | **5p** | ✅ |
| **B2** | **Fault tolerance** | **5p** | ✅ implementat, **de demonstrat** la prezentare |
| **B3** | **Encrypted matching** | **5-10p** | ✅ implementat, **de demonstrat** la prezentare |

---

## 8. TODO list pentru finalizare

### Critic (înainte de predare)
- [ ] **Rulează `./scripts/run-demo.sh eval 180`** pe mașina locală (durată ~7 min)
- [ ] **Completează `docs/EVALUATION_REPORT.md`** cu cifrele din `eval-results.csv`
- [ ] Verifică că rezultatele pe `100% =` vs `25% =` sunt rezonabile (B trebuie > A pentru rata de match)
- [ ] Specifică în raport CPU-ul real (`cat /proc/cpuinfo | grep "model name" | head -1`)

### Pentru prezentare
- [ ] Exportă diagramele Mermaid ca PNG (folosește [mermaid.live](https://mermaid.live))
- [ ] Convertește `PRESENTATION.md` în PDF/PPTX cu pandoc sau Marp
- [ ] Pregătește demo live: 3 terminale (`./run-demo.sh demo`, `./run-demo.sh encrypted`, `./run-demo.sh fault`)
- [ ] Înțelege fluxul PartialMatch pentru întrebări (vezi `sequence-diagram.mermaid`)

### Opțional (dacă mai e timp)
- [ ] Logging cu SLF4J real (nivel WARN pentru producție)
- [ ] Configurare externă (`application.properties`) în loc de Config.java
- [ ] Graceful shutdown ordonat (evită RejectedExecutionException la stop)
- [ ] Containerizare Docker pentru demo

---

## 9. Întrebări probabile la prezentare

**Q: De ce ai folosit consistent hashing și nu round-robin?**
A: Determinism. Cu round-robin, predicatele aceluiași câmp ar ajunge la brokeri diferiți între rulări. Consistent hashing garantează că `company` ajunge mereu la același broker, ceea ce simplifică matching-ul (un singur broker știe TOATE predicatele pe `company`).

**Q: Cum garantezi că nu se pierd notificări la failure?**
A: Replicare proactivă — la fiecare subscripție nouă, broker-ul o trimite la TOȚI peers ca `BrokerState`. Fiecare broker păstrează `replicatedSubs[failedBrokerId]` și absorb-uiește subscripțiile la detectarea failure-ului.

**Q: De ce Protobuf în loc de JSON?**
A: (1) Mesaje binare 5-10× mai compacte; (2) Parsing cu cod generat, fără reflection; (3) Forward/backward compatibility prin numerotare câmpuri; (4) Tip-safety la compilare în Java.

**Q: Cum funcționează matching-ul pe date criptate?**
A: SHA-256 deterministic cu salt comun. Subscriberul hash-uiește `"Google"`; publisher-ul hash-uiește `pub.company`. Brokerul compară `Arrays.equals(hash1, hash2)`. Range operators (`<`, `>`) nu funcționează pe hash-uri (proprietate fundamentală a hash criptografic) — pentru ele am putea folosi OPE (Order-Preserving Encryption) ca extindere viitoare.

**Q: De ce nu folosești Kafka/RabbitMQ?**
A: Cerința specifică explicit că Kafka poate fi folosit DOAR pentru transport, nu pentru matching/storage. Am preferat să implementez și transportul propriu cu TCP sockets, pentru control total. Per Amdahl's law, ar fi adăugat overhead fără beneficiu pentru loopback testing.

**Q: Cât de scalabil e sistemul?**
A: Testat până la 10k subscripții cu 10 pub/s. La 100k subscripții ar trebui partition-uri suplimentare. Limita curentă e match-ing-ul O(N) per publicație — pentru scalare la milioane, ar trebui index inversat per câmp (counting algorithm sau bloom filters).

---

## 10. Locații importante de fișiere

| Fișier | Path |
|---|---|
| Cod sursă principal | `src/main/java/ebs/` |
| Schema Protobuf | `src/main/proto/ebs.proto` |
| Teste unitare | `src/test/java/ebs/` |
| Documentație | `docs/` |
| Script demo | `scripts/run-demo.sh` |
| Maven config | `pom.xml` (produce `target/pubsub-1.0.jar` shaded) |

---

## 11. Date concrete măsurate (post-refactor batched)

Pe Ubuntu / OpenJDK 21, mașină cu 4 cores, `-Xmx2g`:

| Test | Subs | Pub interval | Durata | Pubs Sent | Notifs | Notif/pub | Avg Latency |
|---|---|---|---|---|---|---|---|
| eval 100% EQ | 10 000 | 200 ms | 180 s | 1 798 | 803 303 | 446.78 | 31.07 ms |
| eval 25% EQ | 10 000 | 200 ms | 180 s | 1 798 | 3 233 151 | 1 798.19 | 33.39 ms |
| smoke 2k | 2 000 | 200 ms | 30 s | 300 | 26 721 | 89.07 | 5.58 ms |

Istoric (pre-refactor, container limitat — rămas pentru referință):

| Test | Subs | Pub/s | Durata | Pubs Sent | Notifs | Match Rate | Avg Latency |
|---|---|---|---|---|---|---|---|
| smoke 1k | 1 000 | 20 | 10s | 196 | 3 259 | 16.6 | 56 ms |
| smoke 5k | 5 000 | 20 | 15s | 220 | 3 647 | 16.6 | 5 555 ms |
| smoke 10k | 10 000 | 5 | 20s | 327 | 8 348 | 25.5 | 10 226 ms |

> Latențele mari la 5k+ în istoric erau din cauza container-ului limitat **și** a algoritmului non-batched. Pe hardware real cu pipeline-ul batched, latența rămâne sub 50 ms inclusiv la 10k subs / 10 pub/s.

---

## 12. Concepte EBS din curs (pentru referință)

Din cursurile C01-C10 și laboratoarele Lab1-Lab6:

- **Pub/Sub vs Message Queue:** decoupling temporal, spațial, sincronizare
- **Topic-based vs Content-based:** filtrare pe metadată vs pe conținut
- **Broker overlay:** topologie de brokeri intermediari
- **Subscription covering:** dacă sub A satisface tot ce satisface sub B, B e covered
- **Apache Storm:** tools-of-the-trade (NU folosit în acest proiect, dar relevant pentru context EBS)
- **Subscription summary/digests:** pentru rutare eficientă în rețele mari
- **Mobility:** subscriberi care se mută între brokeri

---

## 13. Cum continuăm dintr-o sesiune nouă

Pentru a relua proiectul cu Claude într-o sesiune nouă:

1. Creează un Project nou pe claude.ai (sau folosește unul existent)
2. Atașează ca **knowledge files**:
   - Acest `PROJECT_KNOWLEDGE.md`
   - Opțional: `README.md`, `EVALUATION_REPORT.md`
   - Opțional: fișierele `.java` esențiale (Broker, Main, EvalHarness)
3. În prima conversație din proiect, spune:
   > "Citește PROJECT_KNOWLEDGE.md și hai să continuăm proiectul EBS Pub/Sub. Sunt la pasul [X]."

Claude va avea instant întreg contextul și va putea continua de unde am rămas.

---

**Ultima actualizare:** 2026-05-20 — pipeline-routing fix + sync cu codul curent
**Versiune document:** 1.1

---

## 14. Changelog

### 1.1 (2026-05-20)
- ✅ **Pipeline matching real în multi-broker:** `Broker.handleBrokerState` populează acum `subscriptions` + `subscriberAddresses` + `myPredicates` pe peers, nu doar `replicatedSubs`. Cerința #4 este îndeplinită efectiv, nu doar pe diagramă.
- ✅ **Proto extins:** `BrokerState` are acum `repeated string subscriber_addrs = 3;` (paralel cu `subscriptions`) ca să poată livra direct `Notification` de la peers.
- ✅ **`PersistentSender` recreat** (`src/main/java/ebs/common/PersistentSender.java`) — pool TCP cu retry on stale socket, `closeAll()`.
- ✅ **`.gitignore`** adăugat (target/, out/, `src/main/java/ebs/proto/`, IDE, etc.).
- ✅ **Config sincronizat în doc:** `BROKER_TIMEOUT_MS = 60_000`, `FAILURE_DETECT_GRACE_MS = 30_000`.
- ⚠️ **Notă teste:** Surefire raportează `Tests run: 0` — clasele de test nu sunt JUnit standard în config-ul curent. De adăugat dependență JUnit 5 dacă vrem CI verde.
- ✅ **Build verificat:** `mvn clean package` → BUILD SUCCESS; `java -cp target/pubsub-1.0.jar ebs.Main` → 300 pubs / 9 316 notifs / ~4 ms latență.
