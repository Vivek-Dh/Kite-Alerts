package vivek.example.kite.ams.service

import com.fasterxml.jackson.databind.ObjectMapper
import java.time.Clock
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.jms.core.JmsTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import vivek.example.kite.ams.repository.AlertOutboxRepository
import vivek.example.kite.common.config.CommonProperties

@Service
class OutboxRelayService(
    private val outboxRepository: AlertOutboxRepository,
    @Qualifier("jmsTopicTemplate") private val jmsTopicTemplate: JmsTemplate,
    private val commonProperties: CommonProperties,
    private val objectMapper: ObjectMapper,
    private val clock: Clock
) {
  private val logger = LoggerFactory.getLogger(javaClass)

  @Scheduled(fixedDelayString = "\${ams.outbox.relayIntervalMs}")
  @Transactional
  @Synchronized // Ensures only one thread can execute this method at a time
  fun relayOutboxMessages() {
    val eventsToProcess = outboxRepository.findTop100ByProcessedAtIsNullOrderByCreatedAt()
    if (eventsToProcess.isEmpty()) {
      logger.debug("No unprocessed outbox events found.")
      return
    }

    logger.warn("Found {} unprocessed outbox events to relay.", eventsToProcess.size)

    eventsToProcess.forEach { event ->
      try {
        val changeEvent = event.payload
        val message = objectMapper.writeValueAsString(changeEvent)

        jmsTopicTemplate.convertAndSend(commonProperties.alertDefinitionUpdatesTopic, message)
        logger.debug(
            "Relayed alert change {} event for alert ID {}.",
            changeEvent.alert.getLogKey(),
            changeEvent.alert.id)

        // Mark the event as processed by setting the Instant timestamp
        event.processedAt = clock.instant()
      } catch (e: Exception) {
        logger.error(
            "Failed to process and relay outbox event {}. It will be retried.", event.id, e)
        throw e
      }
    }
    outboxRepository.saveAll(eventsToProcess)
  }
}
