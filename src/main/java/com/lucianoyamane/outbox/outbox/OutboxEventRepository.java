package com.lucianoyamane.outbox.outbox;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;

import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    // -2 is org.hibernate.LockOptions.SKIP_LOCKED; combined with PESSIMISTIC_WRITE on the
    // Postgres dialect it makes Hibernate emit SELECT ... FOR UPDATE SKIP LOCKED, allowing
    // multiple poller instances to run concurrently without contending on the same rows.
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))
    @Query("select o from OutboxEvent o where o.status = com.lucianoyamane.outbox.outbox.OutboxStatus.PENDING order by o.createdAt asc")
    List<OutboxEvent> buscarPendentesParaProcessar(Pageable pageable);
}
