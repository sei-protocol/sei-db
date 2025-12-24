# Sei Protocol Interview Preparation

## II. Deep Dive: Sei Giga Cryptographic Accumulator

### 1. The Core Problem: High-Throughput State Commitments

Sei Giga targets 200K TPS, requiring a commitment primitive that can handle millions of key-value updates per second. Traditional primitives (MPT, IAVL) were bottlenecks due to large tree depth (hashing cost), sequential writes, poor batch support, and lack of efficient proofs. We needed a new, scalable commitment layer.

### 2. The Strategy: Universal Accumulators + Multi-Core Parallelization

**Universal Accumulator Benefits:**

- O(1) size state commitment (32-48 bytes).
- Constant-size membership / non-membership proofs.
- Faster verification for light clients or L2 bridging (single pairing check).

**Delivered Solution:**

A pairing-based universal accumulator on BLS12-381 curves implemented in C using RELIC, supporting incremental updates, membership/non-membership proofs, multi-threaded batch ingestion, and constant-size root commitment, working effectively at massive scale.

### 3. The Secret Sauce: Multi-Core Arithmetic Pipeline

To achieve ~2 minutes for a 500M-key state, we parallelized the core cryptographic arithmetic:

**CPU Parallelization (OpenMP/AVX):** We process millions of element updates in parallel, where each update involves: Hash → Field Element → (α + e_i) mod n → Multiplication into accumulator's exponent. This was achieved using OpenMP, per-thread partial products, and AVX-optimized big integer multiplication.

**Memory Layout & NUMA Locality:** We flattened all state into cache-friendly, aligned, pre-allocated structs to avoid heap churn, which cut ingestion time by 35-40%.

### 4. Membership / Non-Membership Proof System

We implemented constant-size proof systems:

**Membership Proof:** W = V / (α + y). Verification is essentially one pairing comparison: e(W, g_2^(α+y)) = e(V, g_2).

**Non-Membership Proof:** Used a modified complement set trick, keeping proofs constant-size and verification time extremely fast (microsecond-level).

### 5. Performance Achievements (Measured)

- **State Ingestion:** Aggregated a 500M key-value pair state into a single commitment V in ~2 minutes on a 32-core machine.
- **Scaling:** Updates scale almost linearly with CPU cores due to the embarrassingly parallel multiplication/exponentiation pipeline.
- **Proof Generation:** Witness generation is microsecond-level.

### 6. Benchmarking Framework (Selection of Optimal Design)

We built a comprehensive testbed to select the optimal design by evaluating: RSA accumulator, Pairing-based universal accumulator, Boneh-Drijvers no-setup accumulator, Hash-based Sparse Merkle Trees, and Vector commitments.

**Key Metrics:** Update throughput, Proof generation latency, Multi-core scaling, Memory footprint, and Verification cost.

### Interview Pitch Summary

"For Sei Giga, I designed and built a high-performance universal pairing-accumulator capable of committing 500M key-value pairs in ~2 minutes. I implemented the accumulator in C using RELIC, optimized big-integer arithmetic, pairing operations, and built a multi-core ingestion pipeline where each thread processes elements, computes (α+e_i) mod n, and aggregates partial products. I also implemented the full membership and non-membership proof system, keeping proofs constant-size and verification cost down to a single pairing check. On top of that, I built a benchmarking framework evaluating hash-based trees, pairing-based accumulators, RSA accumulators, and vector commitments to select the optimal design for Sei's 200K TPS next-gen blockchain."

---

## III. Deep Dive: Sei Giga Ledger Data Storage

### S - Situation

Sei Giga is a greenfield, high-performance EVM L1, targeting 200K+ TPS sustained execution and sub-second finality. To achieve this, a new ledger storage pipeline was required, capable of parallel execution, high-throughput WAL, ultra-fast KV writes, and scalable RPC queries, all while maintaining production-grade stability.

### T - Task

My responsibility was to design and implement key parts of the Ledger Data Storage subsystem, including the execution → postblock → storage pipeline, a high-performance Write-Ahead Log (WAL), optimized RocksDB/PebbleDB layout, a scalable MVCC store for historical queries, and specialized receipt/block storage. Goal: enable 200K TPS without compromising safety.

### A - Actions

#### 1. Engineered the High-Throughput Ledger Pipeline

**Parallel Execution (OCC-based):** Built a speculative execution model using Optimistic Concurrency Control (OCC). Transactions are executed in parallel using NumCPU workers, with read/write sets tracked via MVMemoryCollection. Conflict resolution involves re-executing only conflicting subsets.

**Resource Optimization:** Implemented object pooling for Host Contexts and EVM instances to significantly reduce GC pressure in the hot path.

#### 2. Designed the Asynchronous Post-Block Persistence Pipeline

To prevent I/O from stalling execution, we decoupled persistence:

- **Fan-Out:** A worker pool (4+) parallelizes CPU-heavy work (MsgPack serialization, hashing, producing DB changesets).
- **Ordered Fan-In:** An OrderingBuffer enforces block-height monotonicity before writes.
- **Async DB Write Path:** Writes are non-blocking. Changesets are batched by dedicated background goroutines, fully saturating NVMe bandwidth without stalling execution.

#### 3. Implemented a Custom, High-Throughput Write-Ahead Log

Key innovations for blockchain write patterns:

- Parallel serialization & compression before acquiring the WAL lock.
- Group commit strategy for efficiency.
- Self-pruning segments based on time/size.
- Relied on WAL durability to disable DB Sync for performance.

#### 4. Optimized PebbleDB/RocksDB for Giga Workloads

- **DB-Level Tuning:** Tuned the KV engine for maximum write throughput (disabled sync writes for intermediate states, pipelined writes, optimized bloom filters).
- **MVCC Versioned Storage:** Implemented a custom versioned store (key + version) on top of KV engines, enabling zero-cost historical RPC lookups without requiring archive nodes.
- **Dedicated ReceiptDB:** Architected a separate, append-only ReceiptDB with its own parallelized async writer pool, isolating massive receipt I/O from critical state updates.

#### 5. Node Role Separation for Scalability

Separated node functions to improve validator stability:

- **Validators:** Run in a lightweight mode (no BlockDB, no ReceiptWAL) focusing solely on consensus and keeping only the current state.
- **Execution Nodes:** Maintain the full historical ledger and serve RPC, allowing for horizontal scalability of data access capacity.

#### 6. Accelerated RPC & Query Layer

- **Direct Store Access:** BlockAPI and TxAPI bypass the EVM for reads, dramatically reducing overhead.
- **LedgerCache:** Keeps recent blocks & receipts in RAM for zero disk I/O on hot queries.
- **Secondary Indexing:** Indexes (e.g., TxHash → BlockNumber) enable O(1) transaction lookup.

### R - Results

- **Performance:** Supports 200K TPS sustained execution with no execution stalls from I/O. Custom WAL throughput is 3-5× faster than generic implementations.
- **Architectural Strength:** Fully parallelized, asynchronous pipeline that scales across cores and nodes.
- **RPC:** Sub-millisecond lookups for hot data and fast historical queries via MVCC.

### 30-Second STAR Summary (Greenfield Version)

"Sei Giga is a brand-new 200K TPS EVM L1. I designed major components of its ledger storage layer: a parallel OCC execution engine, an async postblock pipeline, a high-performance WAL, tuned RocksDB/PebbleDB layouts, and a versioned MVCC store for historical queries. I also implemented node role separation and accelerated RPC paths. This storage architecture enables Sei Giga to sustain high throughput with stable consensus and fast data access."

---

## IV. Deep Dive: Sei V2 (EVM) Optimization

### 1. Core Problem: EVM State Sync Bottlenecks

Early on, Sei V2 (EVM L1) experienced unstable block-sync throughput (1-2 blocks/sec), high peer churn, and severely long cold-start/snapshot times (3 hours for snapshot, 50 min for cold-start replay), which degraded validator stability and operator experience.

### 2. Reliability Fix: Blocksync-Peer Architecture + Peer Scoring

- **Blocksync-Peer Tiering:** Separated "gossip peers" from specialized "blocksync peers" chosen based on bandwidth and latency heuristics for historical data transfer.
- **Adaptive Peer Scoring:** Implemented a granular scoring system (speed, timeout ratio, continuity) to deterministically select stable, fast peers.
- **Impact:** Block sync throughput stabilized to 6-8 blocks/sec across the network.

### 3. Snapshot Creation: From 3 Hours → 15 Minutes

Snapshot creation was bottlenecked by random I/O and single-thread traversal.

- **Pipelined Snapshot Writing:** Replaced synchronous "walk + write" with a multi-threaded producer → writers design, parallelizing serialization and disk I/O.
- **Prefetching Tree Files:** Explicitly preloaded tree files into the page cache using OS hints (MADV_SEQUENTIAL, WILLNEED) before writing.
- **Impact:** Snapshot rewrite time dropped from ~3 hours → ~15 min.

### 4. Cold-Start Replay: From 50 Minutes → 10 Minutes

Cold-start was dominated by costly random reads from sparse KV files.

- **Sequential Snapshot Prefetch:** Replaced random reads with streaming reads, enabling OS sequential readahead and massive latency reduction.
- **Selective Prefetch:** Only pre-fetched the largest, hottest trees (evm/bank/accounts), skipping sparse files.
- **Page Cache Awareness:** Logic to skip prefetch if the snapshot was already >80% cached, avoiding unnecessary work.
- **Impact:** Cold-start replay time was cut from 45-50 min → ~10 min (4× - 25× speedup).

### Interview Pitch Summary

"I re-architected Sei v2's sync and snapshot pipeline. I redesigned peer scoring to stabilize block sync at 6-8 blocks/sec. Then I rewrote snapshot creation using a pipelined multi-threaded model and OS-level prefetch hints, reducing snapshot time from 3 hours to 15 minutes. Finally, I optimized cold-start with sequential prefetch and adaptive cache detection, cutting replay time from ~50 min to ~10 min. These changes massively improved node operator UX and validator reliability."

---

## V. Deep Dive: GBS — STAR Version (Wyndham Distributed Cache)

### S - Situation

The SynXis Property Hub suffered from 5-10 second query latency for guest reservation data retrieved directly from an Oracle CRS DB (Wyndham ~9,000 hotels). This was causing critical front-desk delays and became a P0 contract risk for the Wyndham partnership. The system needed to scale to 10× (~100M keys, ~180GB cache) while providing sub-second access and advanced search capabilities.

### T - Task

Design and lead the implementation of a low-latency, high-availability, distributed Redis cache service to provide sub-second access to reservations, support typeahead search and multi-attribute filtering, and scale to 10× while maintaining data ingestion for ~2.4M records daily.

### A - Actions

#### 1. Designed a Sharded, High-Availability Redis Architecture

Used consistent hashing (hotel_id + timestamp) for efficient routing across shards. Implemented Governance + 2 Replicas with Sentinel for automatic failover and high availability, using RDB + AOF for durability.

#### 2. Built a Dual-Pipeline Ingestion Framework

Implemented two ingestion paths:

- **Bulk Import:** Load today + tomorrow's reservations.
- **Real-time Updates:** Dual write (DB + cache) for check-ins < 2 days, otherwise DB-only with async cache update. Added a double-deletion delay for strong eventual consistency.

#### 3. Developed a High-Performance Hybrid Search Index (Key Innovation)

Evaluated pure inverted index and ElasticSearch, settling on a Hybrid Indexing strategy:

- **Permutation Index:** Permute only first_name + last_name for fast prefix search.
- **Inverted Index:** Store all other attributes in Redis Sets.
- **Final Search:** Permutation hit ∩ attribute intersections. Achieved P99 < 100ms including typeahead.

#### 4. Built gRPC APIs

Chose gRPC over REST for lower latency and compact binary format for bulk/CRUD/search/unindexing APIs.

#### 5. Leadership and Conflict Resolution

- Prevented PM scope creep by proposing phased delivery.
- Drove technical choices (Redis >> ElasticSearch, gRPC >> REST) with data-driven comparisons.
- Mentored junior engineers through the lifecycle.

### R - Results

- **Performance:** Reduced query latency from 5-10 seconds → < 1 second. Search + autocomplete < 100ms P99.
- **Scalability:** Successfully deployed to 1,000+ hotels, supporting 10× scale and 2.4M+ daily operations.
- **Business Impact:** Eliminated Wyndham's P0 escalation, securing the continuity of a major enterprise contract.

### 30-Second STAR Summary (for behavioral rounds)

"Wyndham was experiencing 5-10 second reservation queries, which became a P0 contract risk. I led the design of a distributed, sharded Redis cache with high availability, bulk + realtime ingestion, gRPC APIs, and a hybrid search index. This reduced latency to under 1 second for 1,000+ hotels, handled 2.4M daily operations, and scaled 10×. Our solution eliminated Wyndham's precondition and secured long-term partnership."

---

## VI. Deep Dive: Pryon Swarm (Amazon ASR Runtime)

### S - Situation

Amazon's ASR engine suffered from significant complexity: unpredictable latency, CPU thrashing due to oversubscribed threads, and poor hardware utilization (40 pre-allocated threads, 12 effective). Blocking tasks (I/O) and CPU-heavy tasks (DNN scoring) were mixed, leading to poor cache/coherency and performance bottlenecks at 3K QPS. A unified concurrency layer was required.

### T - Task

Design and contribute to a new task-based parallel execution library ("Pryon Swarm") to provide a unified, global view of resource allocation, improve hardware efficiency (avoid oversubscription, utilize cores evenly), and improve latency and throughput for ASR pipelines at 3K QPS.

### A - Actions

#### 1. Designed a Virtual Hardware Layer

Created VirtualHardware, a first-class abstraction that understands NUMA topology and supports pinning workers to specific cores (AFFINITY), forming the foundation for global scheduling and monitoring.

#### 2. Built an Elastic Caching Thread Pool

Implemented a custom, elastic, caching, work-stealing-like thread pool. Threads spawn only as needed and idle threads are cached, eliminating the old "40 fixed threads" model and preventing oversubscription.

#### 3. Introduced a Hierarchical Scheduling Model

Split the runtime into two strictly bounded categories:

- **Blocking Workers:** For I/O waits (socket reads, audio ingestion). Many can exist without harming latency.
- **Processing Workers:** For CPU-heavy tasks (DNN scoring, decoding). Strictly bounded and often pinned to cores for cache locality.

This division ensured blocking tasks no longer steal CPU from heavy compute tasks.

#### 4. Implemented a Robust Task System

Used lock-free queues (SPSC, MPMC, MPSC) based on high-performance algorithms to dramatically reduce context-switching and lock contention at high QPS.

#### 5. Thread Pinning & Data Locality Optimization

For performance-critical pipelines (DNN scorer):

- Pinned threads to the same physical cores for the system's lifetime.
- Increased L1/L2 cache reuse and reduced memory load/stores.
- Improved CPI (cycles per instruction) significantly.

### R - Results

- **Performance Improvements:** 3× reduction in ASR latency end-to-end. CPU utilization became predictable and stable performance at 3K QPS.
- **Resource Efficiency:** Moved from 40 static threads → elastic model, increasing effective hardware utilization.
- **Architecture Quality:** Swarm became the unified concurrency model across all ASR pipelines.
- **Reusability:** Swarm became the standard concurrency runtime for multiple Amazon ASR teams.

### 30-Second STAR Summary

"ASR suffered from thread oversubscription and a fragmented concurrency model. I designed Pryon Swarm, a task-based parallel runtime with virtual hardware abstraction, elastic thread pools, lock-free queues, and thread affinity. This unified the scheduling across the ASR pipeline, improved CPU locality, and reduced latency by 3×. Swarm now powers multiple ASR components at Amazon and supports over 3K QPS and 200M+ daily events."
