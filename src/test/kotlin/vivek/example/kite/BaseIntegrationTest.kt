package vivek.example.kite

import java.io.File
import java.nio.file.Files
import java.time.Duration
import org.awaitility.Awaitility.await
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.test.context.junit.jupiter.SpringExtension
import vivek.example.kite.ams.shard.ShardManager
import vivek.example.kite.common.service.SymbolService

@SpringBootTest
@ExtendWith(SpringExtension::class)
abstract class BaseIntegrationTest {
  private val logger = LoggerFactory.getLogger(this::class.java)

  @Autowired protected lateinit var symbolService: SymbolService

  @Autowired protected lateinit var shardManager: ShardManager

  companion object {

    protected lateinit var testPath: String

    @JvmStatic
    @DynamicPropertySource
    fun registerProperties(registry: DynamicPropertyRegistry) {
      val tempDir = Files.createTempDirectory("kite-alerts-test-")
      testPath = tempDir.toAbsolutePath().toString()
      registry.add("ams.rocksDb.basePath") { testPath }
    }
  }

  @BeforeEach
  open fun waitForDataInitialization() {
    // DataInitializer runs on startup. We must wait for all mock alerts to be created
    // and propagated through the outbox pattern to the L1 cache before running any tests.
    val totalMockAlerts = symbolService.getAllSymbols().size * 5 // Mock repo creates 5 per symbol
    logger.info("Waiting for data initialization. Expecting $totalMockAlerts alerts...")

    try {
      await()
          .withPollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(30)) // Increased from 15 to 30 seconds
          .until {
            val allCachedAlerts =
                symbolService
                    .getAllSymbols()
                    .flatMap { symbol ->
                      shardManager.getShardForSymbol(symbol)?.cache?.getActiveAlerts() ?: emptySet()
                    }
                    .toSet()
            logger.debug(
                "Found ${allCachedAlerts.size} alerts in cache, expecting $totalMockAlerts")
            allCachedAlerts.size >= totalMockAlerts
          }
      logger.info("Data initialization completed successfully")
    } catch (e: Exception) {
      logger.error("Data initialization failed: ${e.message}")
      throw e
    }
  }

  @AfterEach
  fun cleanup() {
    val dbPath = File(testPath)
    logger.info("Found path {}", dbPath.absolutePath)
    if (dbPath.exists()) {
      logger.info("Deleting test database at {}", dbPath.absolutePath)
      dbPath.listFiles()?.forEach { file ->
        if (file.isDirectory) file.deleteRecursively() else file.delete()
      }
    }
  }
}
