package com.lucianoyamane.outbox.outbox;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxEventTest {

    @Test
    void novoPendenteComecaComStatusPendingEZeroTentativas() {
        UUID aggregateId = UUID.randomUUID();

        OutboxEvent evento = OutboxEvent.novoPendente(aggregateId, "Usuario", "UsuarioCadastrado", "{}");

        assertThat(evento.getId()).isNotNull();
        assertThat(evento.getAggregateId()).isEqualTo(aggregateId);
        assertThat(evento.getAggregateType()).isEqualTo("Usuario");
        assertThat(evento.getEventType()).isEqualTo("UsuarioCadastrado");
        assertThat(evento.getPayload()).isEqualTo("{}");
        assertThat(evento.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(evento.getAttempts()).isZero();
        assertThat(evento.getProcessedAt()).isNull();
    }

    @Test
    void marcarComoProcessadoAtualizaStatusEDataDeProcessamento() {
        OutboxEvent evento = OutboxEvent.novoPendente(UUID.randomUUID(), "Usuario", "UsuarioCadastrado", "{}");
        Instant quando = Instant.now();

        evento.marcarComoProcessado(quando);

        assertThat(evento.getStatus()).isEqualTo(OutboxStatus.PROCESSED);
        assertThat(evento.getProcessedAt()).isEqualTo(quando);
    }

    @Test
    void registrarTentativaFalhaIncrementaTentativasEMantemPendente() {
        OutboxEvent evento = OutboxEvent.novoPendente(UUID.randomUUID(), "Usuario", "UsuarioCadastrado", "{}");

        evento.registrarTentativaFalha();
        evento.registrarTentativaFalha();

        assertThat(evento.getAttempts()).isEqualTo(2);
        assertThat(evento.getStatus()).isEqualTo(OutboxStatus.PENDING);
    }
}
