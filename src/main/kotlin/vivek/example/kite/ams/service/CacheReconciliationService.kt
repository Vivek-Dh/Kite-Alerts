package vivek.example.kite.ams.service

import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Profile
import org.springframework.data.domain.PageRequest
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import vivek.example.kite.ams.config.AmsProperties
import vivek.example.kite.ams.repository.PostgresAlertRepository
import vivek.example.kite.ams.shard.ShardManager

// State holder for an ongoing, paginated reconciliation process for a single shard.
data class ReconciliationState(
    var cursor: Instant = Instant.EPOCH,
    val activeIdsFromL3: MutableSet<UUID> = mutableSetOf()
)

@Service
@Profile("local")
class CacheReconciliationService(
    private val postgresAlertRepository: PostgresAlertRepository,
    private val rocksDbService: RocksDbService,
    private val shardManager: ShardManager,
    private val amsProperties: AmsProperties
) {
  private val logger = LoggerFactory.getLogger(javaClass)
  // A map to hold the state of the reconciliation process for each shard across multiple scheduled
  // runs.
  private val reconciliationStates = ConcurrentHashMap<String, ReconciliationState>()

  @Scheduled(fixedDelayString = "\${ams.reconciliation.intervalMs}", initialDelay = 0)
  @Transactional(readOnly = true)
  fun reconcileAllShards() {
    logger.debug("Starting periodic cache reconciliation for all shards...")
    val allAssignments = shardManager.getAllAssignments()
    if (allAssignments.isEmpty()) {
      logger.warn("No shards are initialized. Skipping reconciliation.")
      return
    }

    // Process one page for each shard in a single scheduled run.
    allAssignments.forEach { (shardName, assignedSymbols) ->
      reconcileShardPage(shardName, assignedSymbols)
    }
    logger.debug("Finished one cycle of periodic cache reconciliation.")
  }

  private fun reconcileShardPage(shardName: String, assignedSymbols: List<String>) {
    if (assignedSymbols.isEmpty()) return

    // Get or create the state for this shard's reconciliation process.
    val state =
        reconciliationStates.computeIfAbsent(shardName) {
          logger.debug(
              "[{}] No ongoing reconciliation found. Starting a new one from the beginning.",
              shardName)
          ReconciliationState()
        }

    logger.debug("[{}] Continuing reconciliation from cursor: {}", shardName, state.cursor)

    val page =
        postgresAlertRepository.findByIsActiveTrueAndStockSymbolInAndCreatedAtAfterOrderByCreatedAt(
            assignedSymbols, state.cursor, PageRequest.of(0, amsProperties.reconciliation.pageSize))

    if (page.hasContent()) {
      val pageContent = page.content
      pageContent.mapTo(state.activeIdsFromL3) { it.id }
      state.cursor = pageContent.last().createdAt!! // Update the cursor for the next run
      logger.debug(
          "[{}] Processed page of {} active alerts. New cursor is {}. More pages to fetch: {}",
          shardName,
          pageContent.size,
          state.cursor,
          page.hasNext())
    }

    // If this was the last page, the L3 scan is complete for this cycle.
    if (!page.hasNext()) {
      logger.debug(
          "[{}] L3 scan complete for this cycle with a total of {} active alerts found.",
          shardName,
          state.activeIdsFromL3.size)

      val allAlertsInL2 = rocksDbService.getAllAlerts(shardName)
      val allAlertsMapL2 = allAlertsInL2.associateBy { it.id }
      logger.debug(
          "[{}] Found {} total alerts in L2 cache for comparison.", shardName, allAlertsMapL2.size)

      // Reconcile Path A: Find and delete stale alerts from L2
      var staleCount = 0
      allAlertsMapL2.values.forEach { alertInL2 ->
        if (alertInL2.isActive && alertInL2.id !in state.activeIdsFromL3) {
          rocksDbService.deleteAlert(shardName, alertInL2)
          staleCount++
        }
      }

      logger.info("[{}] Finalized reconciliation. Stale alerts removed: {}", shardName, staleCount)

      // CRITICAL: Remove the state to ensure the next full reconciliation starts from scratch.
      reconciliationStates.remove(shardName)
    }
  }
}
