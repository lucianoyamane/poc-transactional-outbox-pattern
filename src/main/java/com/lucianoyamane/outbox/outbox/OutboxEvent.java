package com.lucianoyamane.outbox.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_event")
public class OutboxEvent {

    @Id
    private UUID id;

    @Column(name = "aggregate_type", nullable = false)
    private String aggregateType;

    @Column(name = "aggregate_id", nullable = false)
    private UUID aggregateId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false)
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OutboxStatus status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "processed_at")
    private Instant processedAt;

    @Column(nullable = false)
    private int attempts;

    protected OutboxEvent() {
        // JPA
    }

    private OutboxEvent(UUID id, String aggregateType, UUID aggregateId, String eventType, String payload,
                         OutboxStatus status, Instant createdAt, Instant processedAt, int attempts) {
        this.id = id;
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
        this.status = status;
        this.createdAt = createdAt;
        this.processedAt = processedAt;
        this.attempts = attempts;
    }

    public static OutboxEvent novoPendente(UUID aggregateId, String aggregateType, String eventType, String payload) {
        return new OutboxEvent(UUID.randomUUID(), aggregateType, aggregateId, eventType, payload,
                OutboxStatus.PENDING, Instant.now(), null, 0);
    }

    public void marcarComoProcessado(Instant quando) {
        this.status = OutboxStatus.PROCESSED;
        this.processedAt = quando;
    }

    public void registrarTentativaFalha() {
        this.attempts++;
    }

    public UUID getId() {
        return id;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public UUID getAggregateId() {
        return aggregateId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getPayload() {
        return payload;
    }

    public OutboxStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }

    public int getAttempts() {
        return attempts;
    }
}
