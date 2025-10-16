package vivek.example.kite.ams.model

import com.fasterxml.jackson.annotation.JsonIgnore
import io.hypersistence.utils.hibernate.type.json.JsonType
import jakarta.persistence.*
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.*
import org.hibernate.annotations.Type
import vivek.example.kite.tickprocessor.model.AggregatedLHWindow

enum class ConditionType {
  GT,
  GTE,
  LT,
  LTE,
  EQ
}

@Entity
@Table(name = "alerts")
data class Alert(
    @Id val id: UUID = UUID.randomUUID(),
    val alertId: String,
    val stockSymbol: String,
    val userId: String,
    @Column(precision = 10, scale = 2) val priceThreshold: BigDecimal,
    @Enumerated(EnumType.STRING) val conditionType: ConditionType,
    var isActive: Boolean = true
) {

  // This annotation ensures JPA/Hibernate never tries to insert or update this column,
  // allowing the database's `DEFAULT` value to be used on creation.
  @Column(insertable = false, updatable = false) var createdAt: Instant? = null

  // This annotation ensures the database trigger is the sole source of updates for this column.
  @Column(insertable = false, updatable = false) var updatedAt: Instant? = null

  // These transient fields are not persisted but are available for serialization in API responses.
  @get:Transient
  @get:JsonIgnore
  val createdAtUtc: LocalDateTime?
    get() = createdAt?.atZone(ZoneOffset.UTC)?.toLocalDateTime()

  @get:Transient
  @get:JsonIgnore
  val updatedAtUtc: LocalDateTime?
    get() = updatedAt?.atZone(ZoneOffset.UTC)?.toLocalDateTime()
}

@Entity
@Table(name = "alert_outbox_events")
data class AlertOutboxEvent(
    @Id val id: UUID = UUID.randomUUID(),
    @Type(JsonType::class) @Column(columnDefinition = "jsonb") val payload: AlertChangeEvent,

    // Changed from Long? to Instant?
    var processedAt: Instant? = null
) {
  @Column(insertable = false, updatable = false) var createdAt: Instant? = null
}

// Not an entity, this is the JSON payload for the outbox event
data class AlertChangeEvent(
    val eventType: String, // CREATE, UPDATE, DELETE
    val alert: Alert
)

data class AlertRequest(
    val stockSymbol: String,
    val userId: String,
    val priceThreshold: BigDecimal,
    val conditionType: ConditionType
)

data class AlertDetail(
    val id: UUID,
    val alertId: String,
    val conditionType: ConditionType,
    val priceThreshold: BigDecimal,
    val isActive: Boolean
)

data class TriggeredAlertEvent(
    val eventId: String,
    val alert: Alert,
    val triggeredAt: Instant,
    val triggeredPrice: BigDecimal,
    val window: AggregatedLHWindow,
    val message: String
) {
  // This transient field is not persisted but is available for serialization in API responses.
  @get:Transient
  @get:JsonIgnore
  val triggeredAtUtc: LocalDateTime
    get() = triggeredAt.atZone(ZoneOffset.UTC).toLocalDateTime()
}
