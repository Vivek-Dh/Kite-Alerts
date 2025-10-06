package vivek.example.kite.ams.shard

import com.google.common.hash.Hashing
import java.nio.charset.StandardCharsets
import java.util.*
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import vivek.example.kite.ams.config.AmsProperties

/**
 * Implements a consistent hashing strategy using a hash ring simulated by a TreeMap. This ensures
 * minimal disruption when shards (nodes) are added or removed from the cluster.
 */
@Component
class ConsistentHashingStrategy(private val amsProperties: AmsProperties) : ShardingStrategy {
  private val logger = LoggerFactory.getLogger(javaClass)
  private val hashFunction =
      Hashing.murmur3_32_fixed() // A high-quality, non-cryptographic hash function
  private val ring = TreeMap<Int, String>()

  override fun assign(
      allItems: List<String>,
      availableShards: List<String>
  ): Map<String, List<String>> {
    if (availableShards.isEmpty()) {
      logger.warn("No available shards for assignment.")
      return emptyMap()
    }

    buildRing(availableShards)

    // Group items by assigning each to its nearest clockwise shard on the ring
    return allItems.groupBy { item -> getShardForItem(item) }
  }

  /**
   * Populates the hash ring with virtual nodes for each physical shard. Using virtual nodes ensures
   * a more uniform distribution of items across shards.
   */
  private fun buildRing(shards: List<String>) {
    ring.clear()
    val virtualNodes = amsProperties.consistentHashing.virtualNodesPerShard
    shards.forEach { shard ->
      repeat(virtualNodes) { i ->
        val virtualNodeName = "$shard-v$i"
        val hash = hashFunction.hashString(virtualNodeName, StandardCharsets.UTF_8).asInt()
        ring[hash] = shard // Map the hash to the real shard name
      }
    }
    logger.info(
        "Built consistent hash ring with ${ring.size} total virtual nodes for ${shards.size} shards.")
  }

  /** Finds the responsible shard for a given item (e.g., stock symbol). */
  private fun getShardForItem(item: String): String {
    if (ring.isEmpty()) throw IllegalStateException("Hash ring is not initialized.")
    val hash = hashFunction.hashString(item, StandardCharsets.UTF_8).asInt()

    // Find the first virtual node key clockwise from the item's hash
    val tailMap = ring.tailMap(hash)
    val shardKey =
        if (tailMap.isNotEmpty()) {
          tailMap.firstKey()
        } else {
          // Wrap around to the beginning of the ring
          ring.firstKey()
        }
    return ring[shardKey]!!
  }
}
