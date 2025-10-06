package vivek.example.kite.ams.shard

import org.slf4j.LoggerFactory
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary

/**
 * A test-specific configuration to override the main ShardingStrategy bean. This allows us to use a
 * simple, predictable assignment logic for our tests, making them deterministic and reliable.
 */
@TestConfiguration
class AmsTestConfig {

  private val logger = LoggerFactory.getLogger(this::class.java)

  /**
   * This bean, marked as @Primary, will be injected instead of the main ConsistentHashingStrategy
   * during tests.
   */
  @Bean
  @Primary
  fun predictableShardingStrategy(): ShardingStrategy {
    logger.info("Using PredictableShardingStrategy for tests")
    return ShardingStrategy { allItems, availableShards ->
      // A simple, predictable strategy for tests:
      // Assign the first N items to the first shard, etc.
      // This guarantees that we have enough symbols in one shard for the concurrency test.
      val sortedShards = availableShards.sorted()
      if (sortedShards.isEmpty()) return@ShardingStrategy emptyMap()

      // Example: Assign first 4 to shard-1, rest to shard-2
      val assignments = mutableMapOf<String, MutableList<String>>()
      sortedShards.forEach { assignments[it] = mutableListOf() }

      allItems.forEachIndexed { index, item ->
        if (index < 4 && sortedShards.isNotEmpty()) {
          assignments[sortedShards[0]]?.add(item)
        } else if (sortedShards.size > 1) {
          assignments[sortedShards[1]]?.add(item)
        }
      }
      assignments
    }
  }
}
