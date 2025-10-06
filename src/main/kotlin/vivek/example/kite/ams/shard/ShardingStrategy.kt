package vivek.example.kite.ams.shard

/** Defines a strategy for assigning items (like stock symbols) to a set of available shards. */
fun interface ShardingStrategy {
  /**
   * Takes a list of all items and a list of available shard names, and returns a map where each
   * shard name is a key and the value is the list of items assigned to it.
   */
  fun assign(allItems: List<String>, availableShards: List<String>): Map<String, List<String>>
}
