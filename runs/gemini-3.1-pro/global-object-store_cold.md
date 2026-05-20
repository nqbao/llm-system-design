---

Design a global object storage system like S3.


---

Here is a comprehensive system design for a global object storage system similar to Amazon S3.

## 1. System Requirements & Assumptions

### Core Features
*   **Create/Delete Buckets:** Logical containers for objects, tied to specific geographic regions.
*   **PUT Object:** Upload an object (bytes to terabytes).
*   **GET Object:** Retrieve an object via a unique key.
*   **DELETE Object:** Remove an object.
*   **High Durability & Availability:** Target 99.99% availability and 99.999999999% (11 9s) durability.
*   **Consistency:** Strong read-after-write consistency for new objects and overwrites.

### Capacity Estimation (Math)
Let's design for a massive-scale system:
*   **Traffic:** 100,000 requests per second (RPS) globally.
    *   80% GETs (80,000 RPS)
    *   20% PUTs (20,000 RPS)
*   **Object Size:** Average 1 MB. (Will range from 1 KB to 5 TB, requiring multipart uploads).
*   **Storage Ingress:** 20,000 PUTs/sec $\times$ 1 MB = **20 GB/s** (160 Gbps).
*   **Storage Egress:** 80,000 GETs/sec $\times$ 1 MB = **80 GB/s** (640 Gbps).
*   **Data Storage (1 Year):** 20 GB/s $\times$ 86,400 sec/day $\times$ 365 days $\approx$ **630 Petabytes / year**.
    *   Assuming 1.4x overhead for Erasure Coding (details below), physical storage is $\approx$ **880 PB / year**.
*   **Metadata Storage:** 20,000 PUTs/sec $\times$ 31.5M sec/year $\approx$ 630 Billion objects/year.
    *   At ~1 KB of metadata per object (bucket, key, size, checksum, pointers), metadata grows at **630 TB / year**. This requires a heavy-duty distributed database.

---

## 2. High-Level Architecture

To achieve massive scale, we must strictly decouple **Metadata** (the "table of contents") from **Data** (the actual bytes). 

1.  **Global Routing (DNS):** Directs users to the closest point of presence (PoP) or directly to the target region of the bucket.
2.  **API Gateway:** Handles authentication, rate limiting, and HTTP request routing.
3.  **Metadata Service:** A strongly consistent, highly available distributed key-value store. It maps `Bucket + Key` to a list of physical `Chunk IDs`.
4.  **Storage / Data Nodes:** Commodity hardware packed with HDDs/SSDs. They do not know about "buckets" or "keys"; they only store and retrieve binary "Chunks" by ID.
5.  **Placement Manager:** Tracks the health and free space of all storage nodes. Decides exactly which racks/nodes get which chunks to satisfy fault tolerance.

### Architecture Diagram

```mermaid
architecture-beta
    group global(cloud)[Global Layer]
    group region(cloud)[Region: US-East]

    service dns(internet)[Global DNS / Route53] in global
    
    service gateway(server)[API Gateway / Load Balancer] in region
    service auth(key)[IAM & Auth Service] in region
    
    service meta_api(server)[Metadata API] in region
    service meta_db(database)[Distributed KV DB (CockroachDB/Spanner)] in region
    
    service place_mgr(server)[Placement Manager] in region
    
    service data_api(server)[Data Router] in region
    service node1(disk)[Storage Node Rack A] in region
    service node2(disk)[Storage Node Rack B] in region
    service node3(disk)[Storage Node Rack C] in region
    
    service gc(server)[Garbage Collector / Repair] in region

    dns :R: gateway
    gateway :R: auth
    gateway :R: meta_api
    gateway :R: data_api
    
    meta_api :R: meta_db
    meta_api :L: place_mgr
    
    data_api :R: node1
    data_api :R: node2
    data_api :R: node3
    
    gc :R: node1
    gc :L: meta_db
    place_mgr :R: node1
```

---

## 3. Component Deep Dive

### A. Data Storage (Data Plane)
We cannot store millions of 1MB files directly on a Linux filesystem (ext4/XFS), as we will run out of inodes and disk seek times will destroy performance.
*   **Append-Only Log Structure:** Storage nodes group small objects into large logical volumes (e.g., 256 MB chunks). When an object is written, it is appended to the current active chunk.
*   **Erasure Coding (EC):** 3x replication is too expensive (300% overhead). We use Reed-Solomon Erasure Coding (e.g., 10+4). 
    *   We split a chunk into 10 data fragments and calculate 4 parity fragments.
    *   These 14 fragments are distributed across 14 different failure domains (different racks/power supplies).
    *   We can lose *any* 4 racks and still reconstruct the data. Storage overhead is only 1.4x (14/10).

### B. Metadata Storage (Control Plane)
The Metadata DB must be strongly consistent. If a user uploads an object and immediately requests a list of objects, the new object must be there.
*   **Tech Choice:** CockroachDB, TiDB, or a custom Paxos/Raft-based key-value store (like DynamoDB).
*   **Schema:** 
    *   `Partition Key:` Hash of `BucketName` (prevents hot spots, but breaks ordered listings).
    *   *Correction for List operations:* S3 allows alphabetical prefix listing. Therefore, Partition Key = `BucketName`, Sort Key = `ObjectKey`. 
    *   *Hot Partition Mitigation:* If a bucket gets too hot, we split the partition by prefixing the internal DB key with a hash salt for high-throughput buckets, merging results on read.
*   **Record Content:** `ObjectKey` -> `[Size, MD5 Checksum, CreationDate, UserID, List of (ChunkID, Offset, Length)]`.

---

## 4. Workflows

### 4.1. PUT Object (Write-Path)
Achieving strong read-after-write consistency requires a two-phase commit strategy.
1.  **Init:** Client sends a PUT request to the API Gateway.
2.  **Authorize:** Gateway verifies IAM policies.
3.  **Allocate:** Gateway asks the Placement Manager: "I need to store 1 MB." Placement Manager returns a list of 14 Storage Node IPs (for 10+4 EC).
4.  **Write Data:** Gateway streams the payload to a primary storage node, which calculates the EC parity bits and pipelines the 14 fragments to the 14 nodes.
5.  **Acknowledge:** Once the storage nodes commit the fragments to disk (in an append-only memory buffer flushed to disk), they ACK to the Gateway.
6.  **Commit Metadata:** *Crucial step.* Only AFTER the data is safely on disk does the Gateway tell the Metadata DB: "Create record `bucket/key` pointing to `Chunk X, Offset Y`". 
7.  **Return:** 200 OK to the client.

### 4.2. GET Object (Read-Path)
1.  Client requests `GET bucket/key`.
2.  Gateway queries the Metadata DB for `bucket/key`.
3.  DB returns the location: `Chunk X, Offset Y, Length Z` and the list of Storage Node IPs holding the fragments.
4.  Gateway requests the specific byte-range from 10 of the 14 storage nodes. (If one node is slow or dead, it requests a parity fragment from an 11th node and computes the missing data on the fly).
5.  Gateway streams the reconstructed data back to the client.

### 4.3. Handling Large Files (Multipart Upload)
A 5 TB file cannot be buffered in memory or sent in a single stream over unreliable internet.
1.  Client initiates Multipart Upload. Metadata DB marks the upload state as `PENDING`.
2.  Client uploads 10 MB parts in parallel. Each part follows the standard PUT Data path.
3.  Metadata DB stores temporary pointers for each part.
4.  Client sends `CompleteMultipartUpload`.
5.  Metadata DB stitches the pointers together into a single logical file and changes state to `COMMITTED`.

---

## 5. Explicit Tradeoffs

| Decision | Tradeoff Made | Justification |
| :--- | :--- | :--- |
| **Erasure Coding over 3x Replication** | **Tradeoff:** Higher compute cost (CPU for parity calc) and higher latency on reads if a node fails. | **Justification:** At Exabyte scale, saving 160% in physical disk costs (hundreds of millions of dollars) vastly outweighs the CPU overhead. (We still use 3x replication for the Metadata DB). |
| **Separating Control and Data Planes** | **Tradeoff:** Increased system complexity. Two network hops to write/read. | **Justification:** Allows independent scaling. Storage nodes can be scaled just for disk capacity; Metadata nodes can be scaled for IOPS. |
| **Append-Only Volumes vs In-Place Updates** | **Tradeoff:** Requires a complex background Garbage Collection process to reclaim space from deleted objects. | **Justification:** Disks are terrible at random writes. Append-only provides sequential IO, maximizing disk throughput, and prevents lock contention during parallel writes. |
| **Partitioning by Bucket vs Hash** | **Tradeoff:** Partitioning by bucket enables fast `LIST` prefixes, but risks a "hot bucket" overwhelming a single DB shard. | **Justification:** Most buckets don't hit physical shard limits. We implement dynamic salting/sharding in the Metadata layer *only* when a specific bucket exceeds a throughput threshold. |

---

## 6. What Could Fail? (Resilience & Mitigation)

1.  **Disk / Node Failure (Bitrot or Hard Crash):**
    *   *Failure:* A disk dies, taking down fragments of millions of objects.
    *   *Mitigation:* A background **Repair Daemon** constantly sweeps the metadata. If a node drops out, the daemon reads the remaining 13 fragments of affected chunks, mathematically reconstructs the lost 14th fragment, and writes it to a new healthy node. It also routinely checks MD5 checksums to fix silent data corruption (bitrot).
2.  **Orphaned Data (Failed PUTs):**
    *   *Failure:* Client uploads 100MB of data to storage nodes, but the network drops before the Metadata DB is updated. Disks fill up with "invisible" data.
    *   *Mitigation:* **Garbage Collector**. Storage chunks contain a reverse index (mapping back to the object key). The GC periodically compares the physical chunk contents with the Metadata DB. If data exists on disk without a corresponding DB entry for > 24 hours, it is deleted.
3.  **Metadata DB Network Partition:**
    *   *Failure:* The region's network splits, and the Metadata DB loses quorum.
    *   *Mitigation:* System falls back to Read-Only mode for the minority partition to prevent split-brain. The API Gateway will reject PUTs with a `503 Service Unavailable` to strictly preserve data consistency. Availability is sacrificed for consistency here (CP over AP).
4.  **Massive Thundering Herd (Viral Object):**
    *   *Failure:* A newly uploaded 10MB image goes viral, resulting in 500,000 GETs/sec to a single Storage Node, melting its network interface.
    *   *Mitigation:* **Read-through Caching**. We place a caching layer (e.g., Redis or Varnish) at the API Gateway or edge CDN (like CloudFront). The API Gateway notices the hot key and serves it from memory, protecting the physical disk nodes.