# smuSQL: When Basic Data Structures Meet Databases
*What happens when you take CS201 data structures too seriously?*

## The "What Were You Thinking?" Project
Ever wondered what would happen if you built a database using the most basic data structures from your first Data Structures and Algorithms class? We did. Instead of using sophisticated B-trees or hash indexes, we went back to basics - way back. Like "Week 2 of DSA" back.

## The Unconventional Four

### BackwardsStack: The Database that Thinks It's a Stack of Papers
*Because sometimes the best way forward is backwards*
```java
private Stack<Map<String, String>> rows;
private static final int BATCH_SIZE = 100;
```
Imagine your database is literally a stack of papers on your desk. Need something from the bottom? Sorry, you'll have to pick up everything above it first! But hey, at least it's memory efficient (35.82MB - the neat freak of our implementations).

**What it actually does:**
- Processes data in batches of 100 (because even bad ideas need optimization)
- Uses a temporary stack for operations (yes, we're playing hot potato with your data)
- Surprisingly memory efficient, albeit a bit slower (2.570s in mixed operations)

### LeakyBucket: The Database that Intentionally Forgets
*Inspired by that one time we left a bucket in the rain*
```java
private Bucket mainBucket;      // The bucket we care about
private Bucket overflowBucket;  // The bucket we care about less
```
Like a bucket with a controlled leak, this implementation actually turned out to be our best performer (2.493s in mixed operations). Sometimes letting go is the best strategy!

**What it actually does:**
- Manages data in two buckets: main (1000 rows) and overflow
- Leaks data when main bucket hits 90% capacity
- Periodically cleans up after itself (every 10000 operations)

### PingPong: The Database that Can't Sit Still
*Because sometimes you need to keep your data fit and active*
```java
private List<Map<String, String>> activeList;    // The "fit" data
private List<Map<String, String>> inactiveList;  // The "lazy" data
private Map<Map<String, String>, Integer> accessCounts;
```
Data bounces between active and inactive lists based on usage. Think of it as a fitness program for your data - use it or lose it! Balanced performance (2.536s) with reasonable memory usage (49.72MB).

### RandomQueue: The Database with Multiple Personality Order
*Four queues are better than one, right?*
```java
private final List<Queue<Map<String, String>>> queues;
private static final int NUM_QUEUES = 4;
```
Distributes data across four queues using hash-based allocation. Like choosing the shortest line at the supermarket, except we're using math! Solid middle-ground performer (2.528s).

## The "Theory vs Reality" Plot Twist
*Where computer science textbooks get it wrong*

### INSERT Operations: The O(1) Lie
**What Theory Promised:**
```
All implementations: O(1)
"Just put it at the top/end! How hard can it be?"
```

**What Actually Happened (99th percentile):**
```
BackwardsStack: 7.324ms  (Stack overflow... of disappointment)
RandomQueue:    4.762ms  (Four queues, four times the chaos)
PingPong:      3.550ms  (Bouncing is expensive)
LeakyBucket:   3.292ms  (Winning by leaking!)
```

### SELECT Operations: The Great Equalizer
**What Theory Promised:**
```
BackwardsStack: O(n)     "Stack traversal? A terrible idea!"
LeakyBucket:   O(m + o)  "Double scanning? Even worse!"
PingPong:      O(a + i)  "Two lists? Not great!"
RandomQueue:    O(n/k)   "Finally, some parallelism!"
```

**What Actually Happened:**
```
Simple Select (50th percentile): Everyone at ~0.220ms
Complex Select (50th percentile): Everyone at ~1.430ms
Plot Twist: They're all the same! 
```

## Memory Games: The Storage Story
```
Implementation | Memory   | What We Expected        | What We Got
--------------|----------|------------------------|-------------
BackwardsStack| 35.82MB  | "Memory nightmare"     | Most efficient!
PingPong      | 49.72MB  | "Double the lists,    | Surprisingly reasonable
             |          | double the memory"     |
LeakyBucket   | 51.25MB  | "Leaky = wasteful"    | Best performer
RandomQueue   | 51.93MB  | "Should be efficient" | Most hungry
```

## Performance Theatre: The Three-Act Play

### Act 1: Population Phase
*Where first impressions are made*
```
LeakyBucket & RandomQueue: 0.491s, 0.492s (The speed demons)
BackwardsStack: 0.560s (Fashionably late)
Memory Usage: All ~7-8MB (Everyone starts humble)
```

### Act 2: Mixed Operations Phase
*Where the real drama unfolds*
```
LeakyBucket: 2.493s (The unexpected hero)
RandomQueue: 2.528s (The consistent performer)
PingPong:    2.536s (The balanced artist)
BackwardsStack: 2.570s (The efficient struggler)
```

### Act 3: Complex Queries
*Where everyone shows their true colors*
```
LeakyBucket: 0.621s (Still winning!)
BackwardsStack: 0.650s (Slow but steady)
Everyone else: Somewhere in between
Plot twist: Some showed negative memory usage 
(We're still trying to figure that one out)
```

## Choose Your Fighter

### BackwardsStack: The Memory Miser
- ✓ Best memory efficiency (35.82MB)
- ✓ Predictable performance patterns
- ⚠ INSERT spikes that'll wake you up at night
- Perfect for: Systems where memory is expensive and latency spikes are funny

### LeakyBucket: The Surprise Champion
- ✓ Best overall performance (2.493s mixed ops)
- ✓ Most consistent latency
- ⚠ Memory usage is... generous
- Perfect for: When you want to explain to your boss why intentionally losing data is actually good

### PingPong: The Balanced Performer
- ✓ Reasonable memory usage (49.72MB)
- ✓ Consistent performance
- ⚠ All that bouncing adds up
- Perfect for: When you can't decide if you want good or bad performance, so you settle for okay

### RandomQueue: The Consistent Mediocrity
- ✓ Predictable performance
- ✓ Good distribution
- ⚠ Highest memory usage (51.93MB)
- Perfect for: When you want to explain distributed systems but only have 5 minutes

## Lessons Learned

1. **The O(1) Myth**
   - Theory: "It's constant time!"
   - Reality: "Constants matter... a lot"

2. **Memory vs Speed**
   - Theory: "More memory = better performance"
   - Reality: BackwardsStack proves less can be more (sometimes)

3. **Complex ≠ Better**
   - Theory: "More sophisticated = more efficient"
   - Reality: LeakyBucket won by literally throwing data away

## Final Thoughts
In a world obsessed with optimization and complexity, our "wrong" implementations taught us something valuable: sometimes the simplest solution isn't just viable - it might be the best option. Even if that solution involves intentionally leaking data or treating your database like a stack of papers.

Remember: The next time someone says "that's not how databases work," you can show them empirical proof that sometimes the worst ideas produce the best results.

*P.S. We're still trying to explain to our professors why the leaky one won.*

## Final Thought
Remember: This isn't just a project about building a database with basic data structures - it's about challenging assumptions and finding out that sometimes the "wrong" way isn't as wrong as you might think. Just look at LeakyBucket - who knew intentionally forgetting data could be a winning strategy?

*Note: No data structures were permanently harmed during this experiment, though some were mildly embarrassed.*