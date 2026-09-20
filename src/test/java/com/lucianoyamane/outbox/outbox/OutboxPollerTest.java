package com.lucianoyamane.outbox.outbox;

import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Pageable;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OutboxPollerTest {

    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    private final OutboxPoller outboxPoller = new OutboxPoller(outboxEventRepository, kafkaTemplate);

    @Test
    void publicaEventoPendenteEMarcaComoProcessado() {
        OutboxEvent evento = OutboxEvent.novoPendente(UUID.randomUUID(), "Usuario", "UsuarioCadastrado", "{}");
        when(outboxEventRepository.buscarPendentesParaProcessar(any(Pageable.class))).thenReturn(List.of(evento));

        SendResult<String, String> sendResult = mock(SendResult.class);
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        outboxPoller.publicarPendentes();

        assertThat(evento.getStatus()).isEqualTo(OutboxStatus.PROCESSED);
        assertThat(evento.getProcessedAt()).isNotNull();
        assertThat(evento.getAttempts()).isZero();
    }

    @Test
    void mantemPendenteERegistraTentativaQuandoPublicacaoFalha() {
        OutboxEvent evento = OutboxEvent.novoPendente(UUID.randomUUID(), "Usuario", "UsuarioCadastrado", "{}");
        when(outboxEventRepository.buscarPendentesParaProcessar(any(Pageable.class))).thenReturn(List.of(evento));
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenThrow(new KafkaException("broker indisponivel"));

        outboxPoller.publicarPendentes();

        assertThat(evento.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(evento.getAttempts()).isEqualTo(1);
    }
}
