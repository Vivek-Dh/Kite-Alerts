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
import org.springframework.jms.core.JmsTemplate
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import org.springframework.test.web.reactive.server.WebTestClient
import vivek.example.kite.BaseIntegrationTest
import vivek.example.kite.ams.model.Alert
import vivek.example.kite.ams.model.AlertRequest
import vivek.example.kite.ams.model.ConditionType
import vivek.example.kite.ams.repository.TriggeredAlertHistoryRepository
import vivek.example.kite.ams.service.AlertManagementService
import vivek.example.kite.common.config.CommonProperties
import vivek.example.kite.tickprocessor.model.AggregatedLHWindow

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@ActiveProfiles("test")
@TestPropertySource(locations = ["classpath:application-test.yml"])
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class DataPropagationIntegrationTests : BaseIntegrationTest() {

  @Autowired private lateinit var alertManagementService: AlertManagementService

  @Autowired private lateinit var objectMapper: ObjectMapper
  @Autowired private lateinit var jmsTopicTemplate: JmsTemplate
  @Autowired private lateinit var commonProperties: CommonProperties
  @Autowired private lateinit var historyRepository: TriggeredAlertHistoryRepository

  private val webTestClient: WebTestClient =
      WebTestClient.bindToServer().baseUrl("http://localhost:9090").build()

  @Test
  fun `should create, trigger, and deactivate alert, propagating state through all layers`() {
    // Step 1: Create a new alert in L3. This will commit immediately.
    val request = AlertRequest("INFY", "test-user-123", BigDecimal("1500.00"), ConditionType.GTE)
    val createdAlert = alertManagementService.createAlert(request)
    val shard = shardManager.getShardForSymbol("INFY")!!

    // Step 2: Wait for creation to propagate to L1. This will now succeed.
    await.withPollInterval(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10)).until {
      shard.cache.getAlertDetails(createdAlert.id)?.isActive == true
    }
    assertTrue(shard.cache.getActiveAlerts().any { it.id == createdAlert.id })

    // Step 3: Trigger the alert.
    val window = AggregatedLHWindow("INFY", 1L, 2L, BigDecimal("1490.00"), BigDecimal("1501.00"), 5)
    jmsTopicTemplate.convertAndSend(commonProperties.aggregatedUpdatesTopic, window) {
      it.apply { setStringProperty("stockSymbol", "INFY") }
    }

    // Step 4: Wait for the triggered alert to be handled and persisted.
    await.atMost(Duration.ofSeconds(10)).until {
      historyRepository.findByUserId("test-user-123").isNotEmpty()
    }
    val history = historyRepository.findByUserId("test-user-123").first()
    assertEquals(createdAlert.id, history.alertId)
    assertEquals(0, BigDecimal("1501.00").compareTo(history.triggeredPrice))

    // Step 5: Wait for the DEACTIVATION event to propagate and the alert to be DELETED from L2.
    await.atMost(Duration.ofSeconds(10)).withPollInterval(Duration.ofSeconds(1)).untilAsserted {
      val l2Alert = getAlertFromRocksDb(shard.name, createdAlert.id.toString())
      assertNull(l2Alert, "Alert should be deleted from RocksDB after deactivation.")
    }

    // Step 6: Verify the alert is now inactive in L1.
    val l1Alert = shard.cache.getAlertDetails(createdAlert.id)
    assertNotNull(l1Alert)
    assertFalse(l1Alert!!.isActive)
    assertFalse(shard.cache.getActiveAlerts().any { it.id == createdAlert.id })
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
    return if (response.status.is2xxSuccessful && response.responseBody != null) {
      objectMapper.readValue(response.responseBody!!, object : TypeReference<Alert>() {})
    } else null
  }
}
