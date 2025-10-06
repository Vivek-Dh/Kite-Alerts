package vivek.example.kite.ams.shard

import vivek.example.kite.ams.service.InMemoryAlertCache

data class Shard(
    val name: String,
    val assignedSymbols: List<String>,
    val cache: InMemoryAlertCache
)
