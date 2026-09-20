CREATE TABLE outbox_event (
    id UUID PRIMARY KEY,
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('PENDING', 'PROCESSED')),
    created_at TIMESTAMP NOT NULL,
    processed_at TIMESTAMP,
    attempts INT NOT NULL DEFAULT 0
);

CREATE INDEX idx_outbox_event_status ON outbox_event (status);
