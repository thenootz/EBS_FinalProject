---
title: "EBS Pub/Sub System"
subtitle: "Content-Based Publish/Subscribe with Advanced Routing"
author: "[Student 1], [Student 2]"
date: "Sesiune 2025"
---

# Slide 1 — Titlu

**Implementare sistem Publish/Subscribe content-based distribuit**

Curs: Sisteme Bazate pe Evenimente
Echipă: [Student 1] + [Student 2]
Tehnologii: Java 17 • Protocol Buffers • AES-GCM • TCP

---

# Slide 2 — Cerința

> Arhitectură pub/sub content-based cu:
> - 1-2 publisheri care emit publicații
> - 2-3 brokeri în overlay
> - 2-3 subscriberi
> - Rutare avansată: subscripții distribuite pe brokeri, pubs prin pipeline
> - Evaluare: 10k subs × 3min × 2 scenarii

**Bonusuri abordate:**
- ✅ Protocol Buffers (serializare binară)
- ✅ Fault tolerance (broker failure)
- ✅ Encrypted matching

---

# Slide 3 — Arhitectura

```
   Publishers          Broker Overlay           Subscribers
  ┌─────────┐         ┌──────────────┐         ┌──────────┐
  │  pub-1  │────────►│   broker-1   │◄────────│  sub-1   │
  └─────────┘         │  owns: value │         └──────────┘
                      └──────┬───────┘
  ┌─────────┐         ┌──────▼───────┐         ┌──────────┐
  │  pub-2  │────────►│   broker-2   │◄────────│  sub-2   │
  └─────────┘         │  owns: date, │         └──────────┘
                      │   variation  │
                      └──────┬───────┘         ┌──────────┐
                      ┌──────▼───────┐         │  sub-3   │
                      │   broker-3   │◄────────│          │
                      │  owns: drop, │         └──────────┘
                      │   company    │
                      └──────────────┘
```

Comunicare: **Protocol Buffers peste TCP**

---

# Slide 4 — Rutare avansată: Consistent Hashing

Fiecare câmp atribuit determinist unui broker:

```java
hash("company")   % 3 → broker-3
hash("value")     % 3 → broker-1
hash("drop")      % 3 → broker-3
hash("variation") % 3 → broker-2
hash("date")      % 3 → broker-2
```

**Distribuția subscripțiilor:**
O subscripție `{(company,=,"Google"); (value,>=,90); (variation,<,0.8)}` este descompusă:
- `(company,=,"Google")` → înregistrat la **broker-3**
- `(value,>=,90)` → înregistrat la **broker-1**
- `(variation,<,0.8)` → înregistrat la **broker-2**

---

# Slide 5 — Pipeline de matching (batched)

```
Publication
    │
    ▼  (round-robin)
[broker-1 = entry] evaluează predicatele câmpurilor proprii ("value")
    │   └─ fast path: dacă brokerul deține TOATE predicatele → livrează direct
    │
    ├──· BatchPartialMatch(pub, is_response=false) → broker-2
    └──· BatchPartialMatch(pub, is_response=false) → broker-3
                       │                              │
                       ▼                              ▼
              [broker-2] evaluează          [broker-3] evaluează
              "date" + "variation"          "drop" + "company"
                       │                              │
                       ▼ BatchPartialMatch(is_response=true, [MatchedSub...])
                       └────────┬───────────────────────────────────────────┘
                               ▼
               [broker-1] (coordinator) agregă voturile
                               │
                               ▼
               Toate predicatele match? → Notification către Subscriber
```

**Volum rețea:** doar **4 envelope inter-broker per publicație** (2 forward + 2 vote-uri), indiferent de numărul de subscripții potrivite. Niciun broker nu face singur tot matching-ul — fiecare procesează doar câmpurile sale.

---

# Slide 6 — Bonus 1: Protocol Buffers

Schema `ebs.proto`:
```protobuf
message Envelope {
  enum Type {
    PUBLICATION = 0; SUBSCRIPTION = 1; NOTIFICATION = 2;
    HEARTBEAT = 3;   PARTIAL_MATCH = 4;  // legacy per-sub
    BROKER_STATE = 5; ACK = 6;
    BATCH_PARTIAL_MATCH = 7;             // batched per pub
  }
  Type type = 1;
  string sender_id    = 2;
  string correlation_id = 3;
  oneof payload {
    Publication       publication         = 4;
    Subscription      subscription        = 5;
    Notification      notification        = 6;
    Heartbeat         heartbeat           = 7;
    PartialMatch      partial_match       = 8;
    BrokerState       broker_state        = 9;
    Ack               ack                 = 10;
    BatchPartialMatch batch_partial_match = 11;
  }
}
```

**Avantaje:**
- Mesaje **5-10× mai compacte** decât JSON
- Parsing rapid (cod generat)
- Forward/backward compatibility prin numerotare câmpuri
- Tip-safety la compilare

---

# Slide 7 — Bonus 2: Fault Tolerance

**Mecanism:**
1. Fiecare broker trimite **heartbeat la 2s** către peers
2. Dacă un peer lipsește **>60s** (cu grace period de 30s la pornire), e marcat DEAD
3. Brokerii rămași **absorb subscripțiile** celui căzut (din replicas locale)
4. Rerutare prin `ConsistentHashRouter.removeBroker()`

**Demo:** `java -cp target/pubsub-1.0.jar ebs.Main --fault-test`
- La t=10s, `broker-2` este oprit
- Brokerii 1 și 3 detectează căderea și redistribuie

---

# Slide 8 — Bonus 3: Encrypted Matching

**Problema:** broker-ul nu trebuie să vadă conținutul publicațiilor

**Soluție:**
- Hash determinist (**SHA-256** + salt) pe valorile câmpurilor
- Conținut publicație: **AES-GCM** (256-bit key)
- Match pe `=`: compară hash-uri, **nu plaintext**
- Subscriber decriptează local cu cheia partajată

**Limitări:**
- Range queries (`<`, `>`) nu funcționează pe hash-uri (limitare fundamentală)
- Pentru order-preserving encryption ar fi necesar OPE algorithm

---

# Slide 9 — Evaluare (cerința c)

**Setup:**
- 10 000 subscripții
- 3 minute feed continuu
- 2 publishers × 5 pub/s = 10 pub/s

**Scenariul A: 100% equality pe `company`**
```
Subscripțiile cu "company" folosesc DOAR "="
```

**Scenariul B: 25% equality pe `company`**
```
Subscripțiile cu "company" folosesc "=" 25%, "!=" 75%
```

---

# Slide 10 — Rezultate evaluare

| Metric                          | 100% =       | 25% =        |
|---------------------------------|--------------|--------------|
| Subscripții înregistrate         | 10 000       | 10 000       |
| Timp înregistrare (ms)          | 711          | 391          |
| Publicații trimise              | 1 798        | 1 798        |
| Notificări livrate              | 803 303      | 3 233 151    |
| **Rata potrivire (notif/pub)**  | **446.78**   | **1 798.19** |
| Latență medie (ms)              | 31.07        | 33.39        |

**Concluzie:**
- Scenariul B (25% =) produce **≈4× mai multe notificări** decât Scenariul A, fiindcă predicatele `!=` sunt mai permisive (9/10 valori se potrivesc, față de 1/10 pentru `=`).
- Latența crește modest (×1.07) chiar și la o creștere de ×4 a fan-out-ului, validezând pipeline-ul batched + back-pressure-ul cu queue mărginită.
- Detalii în `docs/EVALUATION_REPORT.md` (CSV brut + log run).

---

# Slide 11 — Optimizări

1. **`BatchPartialMatch`** — o singură envelopă per peer per publicație (vs ~1 800 înainte), 4 envelope inter-broker per publicație indiferent de fan-out
2. **PersistentSender** — pool de socket-uri TCP (un socket per destinație), evită handshake repetat
3. **ThreadPoolExecutor cu `ArrayBlockingQueue(2048)` + CallerRunsPolicy** — back-pressure naturală, previne OOM-ul observat cu `Executors.newFixedThreadPool()`
4. **TCP_NODELAY + BufferedOutputStream** — latență mică, throughput bun
5. **Round-robin publisher → 1 broker entry** — elimină triplicarea volumului de intrare și livrările duplicate
6. **Fast-path delivery** — când brokerul entry deține toate predicatele subscripției, livrează direct (fără round-trip cu peers)
7. **Subscriber loop-read** cu `setSoTimeout(60s)` — reutilizează conexiunile primite de la brokeri, evită storm-ul de reconnect-uri
8. **Heartbeat one-shot** — bypass la coada principală, evită fals-pozitive la congestiție

---

# Slide 12 — Demo live

**Run-demo.sh:**
```bash
./scripts/run-demo.sh demo
```

1. Pornește 3 brokers (porturi 5001-5003)
2. Pornește 3 subscribers (porturi 7001-7003)
3. Înregistrează 300 subscripții
4. Pornește 2 publishers (round-robin la brokeri)
5. Rulează 30 secunde
6. Afișează statistici (pubs/notifs/latență medie)

**Ce vom vedea:**
- Brokerii își înregistrează câmpurile proprii
- Subscripțiile sunt distribuite per câmp
- Notificările curg către subscribers
- Latențe sub 50 ms

---

# Slide 13 — Concluzii

✅ **Toate cerințele de bază acoperite** (25p)
✅ **Toate cele 3 bonusuri implementate** (15-20p)
✅ **Evaluare empirică validată** pe 10k subscripții × 3 min

**Punctele forte:**
- Rutare cu adevărat distribuită
- Tolerance la failure cu recovery automat
- Privacy prin encryption

**Întrebări?**

---

# Slide 14 — Backup: Detalii implementare

**Cod:** 14 clase Java + 1 schemă Protobuf (`ebs.proto`)
**Build:** Maven → `target/pubsub-1.0.jar` shaded (~2 MB)
**Dependențe runtime (incluse în jar):**
- `protobuf-java` 3.25.1
- `slf4j-api` + `slf4j-simple`

**Suport multi-platform:** Linux, macOS, Windows (cu Java 17+)
