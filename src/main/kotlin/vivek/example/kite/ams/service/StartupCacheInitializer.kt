package vivek.example.kite.ams.service

import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.ApplicationListener
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import vivek.example.kite.ams.repository.PostgresAlertRepository
import vivek.example.kite.ams.shard.ShardManager

/**
 * This service runs once on application startup to solve the "cold start" problem. It ensures that
 * if the L2 cache (RocksDB) is empty, it gets populated from the L3 source of truth (PostgreSQL)
 * before the application starts serving requests.
 */
@Component
@Profile("local")
class StartupCacheInitializer(
    private val shardManager: ShardManager,
    private val postgresAlertRepository: PostgresAlertRepository,
    private val rocksDbService: RocksDbService
) : ApplicationListener<ApplicationReadyEvent> {

  private val logger = LoggerFactory.getLogger(javaClass)

  @Transactional(readOnly = true)
  override fun onApplicationEvent(event: ApplicationReadyEvent) {
    logger.info("Application ready. Performing one-time initial cache check...")
    val allAssignments = shardManager.getAllAssignments()

    allAssignments.forEach { (shardName, assignedSymbols) ->
      if (rocksDbService.getAllAlerts(shardName).isEmpty()) {
        logger.warn(
            "[{}] L2 cache (RocksDB) is empty. Performing a one-time full hydration from L3...",
            shardName)

        // 1. Fetch all active alerts from L3 for this shard
        val activeAlertsFromL3 =
            postgresAlertRepository.findByIsActiveTrueAndStockSymbolIn(assignedSymbols)

        // 2. Populate L2
        activeAlertsFromL3.forEach { alert -> rocksDbService.saveAlert(shardName, alert) }
        logger.info(
            "[{}] Populated L2 cache with {} alerts from L3.", shardName, activeAlertsFromL3.size)

        // 3. Force L1 to reload from the now-hydrated L2
        val shard = shardManager.getShardForSymbol(assignedSymbols.first())
        if (shard != null) {
          shard.cache.initializeCache() // Re-initialize L1
          logger.info("[{}] Successfully re-initialized L1 cache from hydrated L2.", shardName)
        }
      } else {
        logger.info(
            "[{}] L2 cache (RocksDB) already contains data. Skipping initial hydration.", shardName)
      }
    }
    logger.info("Initial cache check complete.")
  }
}
