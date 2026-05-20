---

Design a news feed system like Twitter or Facebook.


---

Designing a news feed system at the scale of Twitter or Facebook is one of the classic distributed systems challenges. The core difficulty lies in the massive asymmetry between reads and writes, and the extreme fanout required for highly connected users (the "Celebrity Problem").

Here is a comprehensive, production-grade system design for a News Feed system.

---

### 1. Scope and Requirements

**Functional Requirements:**
*   Users can publish posts (text and media).
*   Users can view a chronological (or algorithmically sorted) news feed of posts from users they follow.
*   Users can follow/unfollow other users.

**Non-Functional Requirements:**
*   **High Availability:** The system must be highly available. Users should always be able to view a feed, even if it's slightly stale.
*   **Low Latency:** Feed generation must be lightning fast (Target: < 100ms).
*   **Eventual Consistency:** It is acceptable for a post to take a few seconds to appear in a follower's feed.

---

### 2. Capacity Planning & Math

Let's assume a scale similar to Twitter.

*   **Daily Active Users (DAU):** 300 Million
*   **Read Traffic:** Users check their feed on average 100 times a day.
    *   $300M \times 100 = 30 \text{ Billion}$ read requests / day.
    *   **Read QPS:** $30B / 86,400 \approx \textbf{350,000 QPS}$ (Peak $\approx 700,000$ QPS).
*   **Write Traffic:** 1% of DAU post once a day.
    *   $300M \times 0.01 = 3 \text{ Million}$ posts / day.
    *   **Write QPS:** $3M / 86,400 \approx \textbf{35 QPS}$ (Peak $\approx 70$ QPS).
    *   *Observation:* Read-to-write ratio is 10,000:1. The system is overwhelmingly read-heavy.
*   **Storage Estimates:**
    *   Average post text + metadata: 1 KB.
    *   Assume 20% of posts contain media (averaging 1 MB). Average post size $\approx 200 \text{ KB}$.
    *   New storage per day: $3 \text{ Million} \times 200 \text{ KB} \approx \textbf{600 GB / day}$ ($\approx 220 \text{ TB / year}$).
*   **Memory (Cache) Estimates:**
    *   To keep latency low, we cache the pre-computed news feed for active users.
    *   Cache 200 post IDs per active user. (Post ID: 8 bytes, User ID: 8 bytes = 16 bytes per item).
    *   $200 \text{ items} \times 16 \text{ bytes} \times 300M \text{ users} \approx \textbf{96 GB}$ of RAM. (Easily fits in a small Redis cluster).

---

### 3. High-Level Architecture Diagram

```mermaid
architecture-beta
    group client(Client Layer)
    group api(API Layer)
    group write_path(Write Path - Post Publishing)
    group read_path(Read Path - Feed Generation)
    group storage(Storage Layer)

    service app(Mobile/Web App) in client
    service api_gateway(API Gateway) in api

    service post_srv(Post Service) in write_path
    service fanout_srv(Fanout Workers) in write_path
    service kafka(Kafka Event Bus) in write_path

    service feed_srv(Feed Service) in read_path
    service graph_srv(Graph Service) in read_path

    service redis_feed(Redis Feed Cache) in storage
    service db_post(Post DB Cassandra) in storage
    service db_graph(Graph DB) in storage
    service s3_media(Object Store S3) in storage

    app:R --> L:api_gateway
    api_gateway:R --> L:post_srv
    api_gateway:R --> L:feed_srv

    post_srv:R --> L:db_post
    post_srv:R --> L:kafka
    post_srv:R --> L:s3_media

    kafka:R --> L:fanout_srv
    fanout_srv:R --> L:redis_feed
    fanout_srv:R --> L:graph_srv

    feed_srv:R --> L:redis_feed
    feed_srv:R --> L:db_post
    feed_srv:R --> L:graph_srv
    
    graph_srv:R --> L:db_graph
```

*(Note: The diagram separates the heavy read path from the asynchronous write path, which is the core of this design).*

---

### 4. Component Deep Dive & Core System Design

#### A. The Core Dilemma: Push vs. Pull (Fanout)
When User A posts, how do Users B, C, and D see it?

**Approach 1: Fanout-on-Read (Pull)**
When a user opens the app, the system fetches all users they follow, fetches their latest posts, merges, and sorts them.
*   *Pros:* No wasted compute for inactive users.
*   *Cons:* $O(N)$ database queries at read time where $N$ is followees. **Violates our < 100ms latency requirement.**

**Approach 2: Fanout-on-Write (Push)**
When User A posts, the system immediately pushes the Post ID into the pre-computed Redis feeds of all followers. Reads are $O(1)$—just fetch the Redis list.
*   *Pros:* Reads are incredibly fast.
*   *Cons:* **The Celebrity Problem (Justin Bieber effect).** If a user with 100 million followers posts, pushing to 100 million Redis lists takes minutes/hours, causing severe queue backups and delayed feeds.

**The Solution: Hybrid Architecture**
We use a hybrid approach based on a user's follower count.
1.  **Normal Users (Push):** Users with $< 10,000$ followers use Fanout-on-Write. Their posts are pushed to their followers' Redis feeds asynchronously via Kafka.
2.  **Celebrities (Pull):** Users with $> 10,000$ followers do *not* push to followers.
3.  **Read Time Merge:** When User B requests their feed, the Feed Service fetches their pre-computed Redis feed (containing posts from normal users), then queries the Graph DB for celebrities User B follows, fetches the celebrities' recent posts, merges them in memory, and returns the result.

#### B. The Write Path (Publishing a Post)
1.  Client sends `POST /v1/feed` with text and media.
2.  **API Gateway** routes to **Post Service**.
3.  Post Service stores media in **S3**, gets a CDN URL.
4.  Post Service generates a distributed time-sortable ID (e.g., Twitter Snowflake / UUIDv7) for the post.
5.  Post is saved to **Post DB (Cassandra)**.
6.  Post Service fires a `PostCreated` event to **Kafka**.
7.  **Fanout Workers** consume the event. They check the **Graph Service** to see if the user is a celebrity.
    *   If *Celebrity*: Do nothing.
    *   If *Normal*: Fetch follower list, and append the `PostID` to each follower's sorted set in the **Redis Feed Cache**.

#### C. The Read Path (Viewing the Feed)
1.  Client requests `GET /v1/feed?cursor=...`
2.  **API Gateway** routes to **Feed Service**.
3.  Feed Service fetches the user's pre-computed timeline from **Redis Feed Cache** (List of Post IDs).
4.  Feed Service queries **Graph Service**: "Who are the celebrities this user follows?"
5.  Feed Service queries **Redis Celebrity Cache** (or Post DB) for the latest Post IDs of those celebrities.
6.  Feed Service merges the two lists of Post IDs and sorts by timestamp.
7.  Feed Service queries **Post DB** to "hydrate" the Post IDs with actual text, author info, and media URLs.
8.  Returns JSON to the client.

#### D. Data Models

**1. Post Database (Cassandra)**
Why Cassandra? It handles massive write volumes and allows efficient wide-row querying.
*   `user_id` (Partition Key)
*   `post_id` (Clustering Key - Time sorted via Snowflake ID)
*   `content` (Text)
*   `media_urls` (JSON array)
*   `created_at` (Timestamp)

**2. Graph Database (PostgreSQL or specialized Graph DB)**
*   `follower_id`
*   `followee_id`
*   `is_celebrity` (Boolean, denormalized for fast filtering)
*   *Indexes on both `follower_id` and `followee_id`.*

**3. Feed Cache (Redis)**
*   Data Structure: **Sorted Set (ZSET)**.
*   Key: `feed:{user_id}`
*   Score: Timestamp (or Snowflake ID).
*   Value: `post_id`.
*   *We cap this ZSET at 500 items to prevent unbounded memory growth.*

---

### 5. Explicit Tradeoffs Made

1.  **Eventual Consistency vs. Strong Consistency:**
    *   *Tradeoff:* We use Kafka for asynchronous fanout. When a normal user posts, it might take 2-3 seconds to appear in followers' feeds.
    *   *Justification:* Ensuring ACID compliance across millions of followers would bring the system to a halt. Eventual consistency is perfectly acceptable for social media.
2.  **Storage vs. Compute (Pre-computation):**
    *   *Tradeoff:* We store a copy of a `post_id` in the Redis cache of *every single follower* (for non-celebrities).
    *   *Justification:* Memory is cheap; CPU time spent doing table joins at read-time for 350,000 QPS is incredibly expensive. We trade memory for extreme read performance.
3.  **Cassandra vs. Relational DB for Posts:**
    *   *Tradeoff:* We lose easy relational joins (e.g., joining Post with User profile data).
    *   *Justification:* At 3 Million writes and 30 Billion reads a day, a single SQL database will melt. Cassandra provides linear scalability. User profile hydration is done at the application layer via a separate User Service/Cache.

---

### 6. What Could Fail? (Failure Scenarios & Mitigations)

**1. Failure: The Thundering Herd (Celebrity Read Spike)**
*   *Scenario:* A major event happens (e.g., the Super Bowl). Millions of users refresh their feeds simultaneously to see what a specific celebrity posted. The "Pull" part of our hybrid model overwhelms the Post DB.
*   *Mitigation:* Introduce a **Local In-Memory Cache** (e.g., Guava/Caffeine) on the Feed Service nodes specifically for Celebrity posts. If Elon Musk tweets, that tweet is cached locally on every Feed server for 10 seconds. DB hits drop to near zero.

**2. Failure: Redis Cluster Goes Down**
*   *Scenario:* The Redis cluster holding the pre-computed feeds crashes and loses data.
*   *Mitigation:*
    *   Use Redis Cluster with replication and automatic failover.
    *   **Fallback mechanism:** If Redis is empty or down, the Feed Service falls back to a pure "Pull" model. It will query the DB directly. This will be slow (degraded state), but the system remains available. Background workers will asynchronously rebuild the Redis feeds based on recent logins.

**3. Failure: Kafka Lag on Fanout**
*   *Scenario:* A spike in normal user posts causes Kafka consumer lag. Fanout takes minutes instead of milliseconds.
*   *Mitigation:*
    *   Monitor Kafka topic offsets. Configure autoscaling on the Fanout Worker group based on lag metrics.
    *   Implement **priority queues**. Active users get their feeds updated via a high-priority topic, while users who haven't logged in for 30 days are routed to a low-priority topic (or dropped entirely from push, relying on pull when they finally log back in).

**4. Failure: Hot Partitions in Cassandra**
*   *Scenario:* If we partitioned the Post DB purely by time (`YYYY-MM-DD`), all writes for today would hit a single node, melting it.
*   *Mitigation:* This is why the Partition Key is `user_id`. Writes are evenly distributed across the cluster based on who is posting, preventing hot spots. (If a specific user posts thousands of times a second, that's a bot, and the API Gateway rate limiters will block them).