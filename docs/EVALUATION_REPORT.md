# Raport de Evaluare — EBS Pub/Sub System

**Curs:** Sisteme Bazate pe Evenimente
**Universitatea:** Alexandru Ioan Cuza, Iași
**Echipa:** [Nume Student 1], [Nume Student 2]
**Data:** [Data evaluării]

---

## 1. Configurația sistemului testat

### Hardware
- **CPU:** [ex. Intel Core i7-12700H, 14 cores, 20 threads]
- **RAM:** [ex. 32 GB DDR5]
- **OS:** [ex. Ubuntu 24.04 / Windows 11]
- **Java:** OpenJDK 21

### Topologie
- **3 Brokers** (broker-1, broker-2, broker-3) pe localhost:5001-5003
- **2 Publishers** (pub-1, pub-2)
- **3 Subscribers** (sub-1, sub-2, sub-3) pe localhost:7001-7003

### Distribuția câmpurilor pe brokeri (consistent hashing)
- `broker-1` deține: **value**
- `broker-2` deține: **variation, date**
- `broker-3` deține: **company, drop**

### Parametri rulare
- Subscripții totale: **10 000** (distribuite 3 333 per subscriber)
- Durata feed: **180 secunde** (3 minute)
- Interval emisie publicații: **200 ms** per publisher
- Frecvențe câmpuri în subscripții:
  - company: 90%
  - value: 80%
  - drop: 60%
  - variation: 50%
  - date: 40%

---

## 2. Scenarii de evaluare

### Scenariul A — 100% equality pe `company`
- Toate subscripțiile care conțin câmpul `company` folosesc operatorul `=`
- Configurare: `eqFreq.put("company", 1.0)`

### Scenariul B — 25% equality pe `company`
- Doar 25% din subscripțiile care conțin `company` folosesc `=`; restul folosesc `!=`
- Configurare: `eqFreq.put("company", 0.25)`

---

## 3. Rezultate măsurate

### Tabel comparativ

| Metric                              | Scenariu A (100% =) | Scenariu B (25% =) |
|-------------------------------------|---------------------|--------------------|
| Subscripții înregistrate            | 10 000              | 10 000             |
| Timp înregistrare subscripții (ms)  | [REZULTAT]          | [REZULTAT]         |
| Publicații trimise (3 min)          | [REZULTAT]          | [REZULTAT]         |
| Notificări livrate                  | [REZULTAT]          | [REZULTAT]         |
| **Rata de potrivire (notif/pub)**   | **[REZULTAT]**      | **[REZULTAT]**     |
| Latență medie livrare (ms)          | [REZULTAT]          | [REZULTAT]         |
| Throughput publicații (pub/s)       | [REZULTAT]          | [REZULTAT]         |
| Throughput notificări (notif/s)     | [REZULTAT]          | [REZULTAT]         |

> **Notă:** Cifrele vor fi populate după rularea `java -Dfeed.seconds=180 -cp ... ebs.EvalHarness`. Fișierul `eval-results.csv` generat conține valorile exacte.

---

## 4. Interpretarea rezultatelor

### a) Numărul de publicații livrate cu succes (3 min feed)

Cu un interval de publicare de 200ms per publisher × 2 publishers = 10 publicații/secundă × 180 secunde = **~1 800 publicații trimise total**.

Numărul de notificări livrate depinde de cât de selective sunt subscripțiile:
- În scenariul A (100% =), predicatele de tip `company = "X"` se potrivesc doar cu 1 din 10 companii din pool → match rate redus per predicat dar combinabil
- În scenariul B (25% =), majoritatea predicatelor sunt `company != "X"` → match rate ridicat per predicat (9 din 10 companii satisfac `!=`)

### b) Latența medie de livrare

Latența totală include:
1. **Publisher → Broker** (~1-2 ms pe loopback)
2. **Matching local pe broker** (paralelizat, ~0.5-2 ms)
3. **PartialMatch fan-out la peers** (~2-5 ms)
4. **Agregare votes și verificare completare** (~0.5 ms)
5. **Broker → Subscriber notification** (~1-2 ms)

**Total așteptat:** 5-50 ms pe loopback la încărcare moderată; la 10k subscripții și 10 pub/s, latența poate crește la 100-500 ms din cauza acumulării în coadă.

### c) Comparația rate de potrivire

**Predicție teoretică:**
- Pentru câmpul `company` cu pool de 10 companii:
  - `=` are probabilitate ~10% per publicație
  - `!=` are probabilitate ~90% per publicație

Scenariul B (25% =, 75% !=) ar trebui să producă **un număr mult mai mare de matches per publicație** decât scenariul A (100% =), deoarece predicatele `!=` sunt mult mai permissive.

**Așteptare:**
```
Scenariu B / Scenariu A ≈ (0.25 × 0.1 + 0.75 × 0.9) / 1.0 × 0.1 ≈ 7×
```

În practică, raportul poate diferi din cauza interacțiunilor cu celelalte predicate ale subscripției (value, drop, variation, date) care reduc probabilitatea finală de match.

---

## 5. Optimizări implementate

1. **PersistentSender** — pool de conexiuni TCP per (host:port) pentru a evita overhead-ul de handshake
2. **Thread pool per broker** (4-8 threads) pentru procesare paralelă a `Envelope`-urilor primite
3. **`parallelStream()` pe `myPredicates`** — matching distribuit pe toate core-urile CPU
4. **Heartbeat one-shot** — heartbeat-urile folosesc socket-uri scurte ca să nu fie blocate pe pool-ul congestionat
5. **Buffered output streams** (64KB) pentru transmisia de publicații
6. **TCP_NODELAY** activat pentru latență mică

---

## 6. Limitări observate

1. **Latență crescută la încărcare mare** — la 10k subscripții, latența medie poate ajunge la câteva secunde dacă publishers sunt mai rapizi decât poate procesa brokerul
2. **Mecanism PartialMatch best-effort** — în condiții de congestie pot exista corelații care nu se finalizează în timp util
3. **Encryption mode** — operatorii de inegalitate (`<`, `>`, `<=`, `>=`) nu sunt suportați pe câmpuri criptate; sistemul cade înapoi la plaintext pentru aceste predicate

---

## 7. Concluzii

Sistemul implementat satisface toate cerințele de bază ale temei și include cele trei bonusuri opționale (Protocol Buffers, fault tolerance, encrypted matching).

**Punctele forte:**
- Rutare reală distribuită (nu un singur broker face tot matching-ul)
- Tolerare la căderi cu replicare automată
- Serializare binară eficientă cu Protobuf
- Generator de date cu frecvențe exacte (Fisher-Yates)

**Posibile îmbunătățiri viitoare:**
- Migrare de la TCP la UDP/multicast pentru reducerea overhead-ului
- Implementare bloom filters pentru pre-filtrare rapidă
- Bază de date persistentă pentru subscripții (acum totul e in-memory)
- Suport pentru operatori de range pe câmpuri criptate (order-preserving encryption)

---

## Anexă A — Output complet `EvalHarness`

```
[copiază aici output-ul complet al rulării]
```

## Anexă B — Conținut `eval-results.csv`

```csv
[copiază aici conținutul fișierului generat]
```
