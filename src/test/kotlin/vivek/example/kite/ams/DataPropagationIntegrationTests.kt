// package vivek.example.kite.ams
//
// import java.math.BigDecimal
// import java.time.Clock
// import java.time.Duration
// import org.awaitility.kotlin.await
// import org.junit.jupiter.api.Assertions.assertEquals
// import org.junit.jupiter.api.Assertions.assertNotNull
// import org.junit.jupiter.api.Test
// import org.springframework.beans.factory.annotation.Autowired
// import org.springframework.boot.test.context.SpringBootTest
// import org.springframework.test.context.ActiveProfiles
// import vivek.example.kite.ams.model.ConditionType
// import vivek.example.kite.ams.repository.PostgresAlertRepository
// import vivek.example.kite.ams.service.AlertManagementService
// import vivek.example.kite.ams.service.OutboxRelayService
// import vivek.example.kite.ams.shard.ShardManager
//
// @SpringBootTest
// @ActiveProfiles("test")
// class DataPropagationIntegrationTests {
//
//  @Autowired private lateinit var alertManagementService: AlertManagementService
//
//  @Autowired private lateinit var outboxRelayService: OutboxRelayService
//
//  @Autowired private lateinit var shardManager: ShardManager
//
//  @Autowired private lateinit var postgresAlertRepository: PostgresAlertRepository
//
//  @Autowired private lateinit var clock: Clock
//
//  @Test
//  fun `should propagate alert from postgres to L1 cache via outbox pattern`() {
//    val stockSymbol = "TCS"
//    val priceThreshold = BigDecimal("3500.00")
//    val userId = "test_user"
//
//    // Step 1: Create alert in Postgres and outbox
//    val newAlert =
//        alertManagementService.createAlert(
//            vivek.example.kite.ams.model.AlertRequest(
//                stockSymbol = stockSymbol,
//                priceThreshold = priceThreshold,
//                userId = userId,
//                conditionType = ConditionType.GT))
//    assertNotNull(postgresAlertRepository.findById(newAlert.id).orElse(null))
//
//    // Step 2: Manually trigger the outbox relay
//    outboxRelayService.relayOutboxMessages()
//
//    // Step 3: Verify the alert is in the correct L1 cache shard
//    await.atMost(Duration.ofSeconds(5)).untilAsserted {
//      val shard = shardManager.getShardForSymbol(stockSymbol)
//      assertNotNull(shard, "Shard for $stockSymbol not found")
//
//      val cachedAlert = shard!!.cache.getAlertDetails(newAlert.id)!!
//      assertNotNull(
//          cachedAlert, "Alert ${newAlert.id} not found in L1 cache for shard ${shard.name}")
//      assertEquals(newAlert.id, cachedAlert.id)
//    }
//  }
// }

package vivek.example.kite.ams

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import java.math.BigDecimal
import java.time.Duration
import org.awaitility.kotlin.await
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import org.springframework.test.web.reactive.server.WebTestClient
import vivek.example.kite.ams.model.Alert
import vivek.example.kite.ams.model.AlertRequest
import vivek.example.kite.ams.model.ConditionType
import vivek.example.kite.ams.service.AlertManagementService
import vivek.example.kite.ams.shard.ShardManager

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@ActiveProfiles("test")
@TestPropertySource(locations = ["classpath:application-test.yml"])
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class DataPropagationIntegrationTests {

  @Autowired private lateinit var alertManagementService: AlertManagementService

  @Autowired private lateinit var shardManager: ShardManager

  @Autowired private lateinit var objectMapper: ObjectMapper

  private val webTestClient: WebTestClient =
      WebTestClient.bindToServer().baseUrl("http://localhost:9090").build()

  @Test
  fun `should propagate alert creation from L3 DB to L1 in-memory cache`() {
    // Step 1: Create a new alert in L3 (Postgres/H2)
    val request =
        AlertRequest(
            stockSymbol = "INFY",
            userId = "test-user-123",
            priceThreshold = BigDecimal("1500.00"),
            conditionType = ConditionType.GTE)
    val createdAlert = alertManagementService.createOrUpdateAlert(request)
    assertNotNull(createdAlert.id)
    assertTrue(createdAlert.isActive)

    val shard = shardManager.getShardForSymbol("INFY")!!
    assertNotNull(shard)

    // Step 2: Immediately verify the alert exists in L3 via API
    val l3Alert = getAlertFromPostgres(createdAlert.id.toString())
    assertEquals(createdAlert.id, l3Alert.id)
    assertEquals("INFY", l3Alert.stockSymbol)
    assertTrue(l3Alert.isActive)

    // Step 3: Wait and verify the alert is propagated to L2 (RocksDB) via API
    await.atMost(Duration.ofSeconds(10)).withPollInterval(Duration.ofSeconds(1)).untilAsserted {
      val l2Alert = getAlertFromRocksDb(shard.name, createdAlert.id.toString())
      assertNotNull(l2Alert)
      assertEquals(createdAlert.id, l2Alert!!.id)
      assertTrue(l2Alert.isActive)
    }

    // Step 4: Verify the alert is hydrated into L1 (In-Memory Cache)
    val l1Alert = shard.cache.getAlertDetails(createdAlert.id)
    assertNotNull(l1Alert)
    assertEquals(createdAlert.id, l1Alert!!.id)
    assertTrue(l1Alert.isActive)
  }

  @Test
  fun `should propagate alert update from postgres to L1 cache via outbox pattern`() {
    // Step 1: Create an initial alert
    val symbol = "WIPRO"
    val userId = "test-user-789"
    val condition = ConditionType.EQ
    val initialPrice = BigDecimal("450.00")

    val initialRequest =
        AlertRequest(
            stockSymbol = symbol,
            userId = userId,
            priceThreshold = initialPrice,
            conditionType = condition)
    val createdAlert = alertManagementService.createOrUpdateAlert(initialRequest)
    val alertId = createdAlert.id

    val shard = shardManager.getShardForSymbol(symbol)!!
    assertNotNull(shard)

    // Step 2: Wait for the initial creation to propagate to L1
    await.atMost(Duration.ofSeconds(10)).withPollInterval(Duration.ofSeconds(1)).until {
      shard.cache.getAlertDetails(alertId)?.priceThreshold?.compareTo(initialPrice) == 0
    }
    assertEquals(0, initialPrice.compareTo(shard.cache.getAlertDetails(alertId)?.priceThreshold))

    // Step 3: Update the alert in L3 by creating a new one and deactivating the old one
    val updatedPrice = BigDecimal("475.50")
    // NOTE: To update, we must match the unique key (alertId + userId). The alertId is derived
    // from symbol, condition, and price. So we use the same details but will get an updated object.
    val updateRequest =
        AlertRequest(
            stockSymbol = symbol,
            userId = userId,
            priceThreshold = updatedPrice, // New price
            conditionType = condition)
    // This will create a NEW alert because the price changed, which is part of the logical key.
    // We then need to deactivate the OLD one.
    val updatedAlert = alertManagementService.createOrUpdateAlert(updateRequest)
    alertManagementService.deactivateAlert(alertId) // Deactivate the old alert

    // Step 4: Verify the update in L3
    val l3AlertNew = getAlertFromPostgres(updatedAlert.id.toString())
    assertEquals(0, updatedPrice.compareTo(l3AlertNew.priceThreshold))
    val l3AlertOld = getAlertFromPostgres(alertId.toString())
    assertFalse(l3AlertOld.isActive)

    // Step 5: Wait and verify the update is propagated to L2 (RocksDB)
    await.atMost(Duration.ofSeconds(10)).withPollInterval(Duration.ofSeconds(1)).untilAsserted {
      val l2Alert = getAlertFromRocksDb(shard.name, updatedAlert.id.toString())
      assertNotNull(l2Alert)
      assertEquals(0, updatedPrice.compareTo(l2Alert!!.priceThreshold))
      val l2OldAlert = getAlertFromRocksDb(shard.name, alertId.toString())
      assertNotNull(l2OldAlert)
      assertFalse(l2OldAlert!!.isActive)
    }

    // Step 6: Verify the update is hydrated into L1 (In-Memory Cache)
    val l1NewAlert = shard.cache.getAlertDetails(updatedAlert.id)
    assertNotNull(l1NewAlert)
    assertEquals(0, updatedPrice.compareTo(l1NewAlert!!.priceThreshold))

    val l1OldAlert = shard.cache.getAlertDetails(alertId)
    assertNotNull(l1OldAlert)
    assertFalse(l1OldAlert!!.isActive)

    // Also verify the old alert is gone from the specific price bucket in the L1 cache
    val symbolCache = shard.cache.getAlertsForSymbol(symbol)
    assertNotNull(symbolCache)

    val oldPriceList = symbolCache!!.equalsAlerts[initialPrice]
    assertTrue(oldPriceList == null || oldPriceList.none { it.id == alertId })

    val newPriceList = symbolCache.equalsAlerts[updatedPrice]
    assertNotNull(newPriceList)
    assertTrue(newPriceList!!.any { it.id == updatedAlert.id })
  }

  @Test
  fun `should propagate alert deactivation from L3 DB to L1 in-memory cache`() {
    // Step 1: Create an active alert
    val request =
        AlertRequest(
            stockSymbol = "TCS",
            userId = "test-user-456",
            priceThreshold = BigDecimal("3500.00"),
            conditionType = ConditionType.LT)
    val createdAlert = alertManagementService.createOrUpdateAlert(request)
    assertTrue(createdAlert.isActive)

    val shard = shardManager.getShardForSymbol("TCS")!!
    assertNotNull(shard)

    // Wait for it to propagate to L1 initially
    await.atMost(Duration.ofSeconds(10)).withPollInterval(Duration.ofSeconds(1)).until {
      shard.cache.getAlertDetails(createdAlert.id)?.isActive == true
    }

    // Step 2: Deactivate the alert in L3
    val deactivatedAlert = alertManagementService.deactivateAlert(createdAlert.id)
    assertFalse(deactivatedAlert.isActive)

    // Step 3: Verify deactivation in L3
    val l3Alert = getAlertFromPostgres(createdAlert.id.toString())
    assertFalse(l3Alert.isActive)

    // Step 4: Wait and verify deactivation is propagated to L2
    await.atMost(Duration.ofSeconds(10)).withPollInterval(Duration.ofSeconds(1)).untilAsserted {
      val l2Alert = getAlertFromRocksDb(shard.name, createdAlert.id.toString())
      assertNotNull(l2Alert)
      assertFalse(l2Alert!!.isActive)
    }

    // Step 5: Verify deactivation is hydrated into L1
    val l1Alert = shard.cache.getAlertDetails(createdAlert.id)
    assertNotNull(l1Alert)
    assertFalse(l1Alert!!.isActive)
  }

  private fun getAlertFromPostgres(alertId: String): Alert {
    val responseBody =
        webTestClient
            .get()
            .uri("/api/v1/alerts/postgres/{alertId}", alertId)
            .exchange()
            .expectStatus()
            .isOk
            .expectBody(ByteArray::class.java)
            .returnResult()
            .responseBody
    assertNotNull(responseBody)
    return objectMapper.readValue(responseBody!!, object : TypeReference<Alert>() {})
  }

  private fun getAlertFromRocksDb(shardName: String, alertId: String): Alert? {
    val response =
        webTestClient
            .get()
            .uri("/api/v1/alerts/rocksdb/{shardName}/{alertId}", shardName, alertId)
            .exchange()
            .expectBody(ByteArray::class.java)
            .returnResult()

    if (response.status.is2xxSuccessful && response.responseBody != null) {
      return objectMapper.readValue(response.responseBody!!, object : TypeReference<Alert>() {})
    }
    return null
  }
}
