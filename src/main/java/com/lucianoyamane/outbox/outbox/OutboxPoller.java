package com.lucianoyamane.outbox.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxPoller {

    private static final Logger log = LoggerFactory.getLogger(OutboxPoller.class);
    private static final int TAMANHO_LOTE = 20;

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxPoller(OutboxEventRepository outboxEventRepository, KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelayString = "${outbox.poller.fixed-delay-ms:2000}")
    @Transactional
    public void publicarPendentes() {
        List<OutboxEvent> pendentes = outboxEventRepository.buscarPendentesParaProcessar(PageRequest.of(0, TAMANHO_LOTE));
        for (OutboxEvent evento : pendentes) {
            publicar(evento);
        }
    }

    private void publicar(OutboxEvent evento) {
        try {
            kafkaTemplate.send(topicoPara(evento.getEventType()), evento.getAggregateId().toString(), evento.getPayload())
                    .get(5, TimeUnit.SECONDS);
            evento.marcarComoProcessado(Instant.now());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Falha ao publicar evento outbox {}: {}", evento.getId(), e.getMessage());
            evento.registrarTentativaFalha();
        } catch (Exception e) {
            log.warn("Falha ao publicar evento outbox {}: {}", evento.getId(), e.getMessage());
            evento.registrarTentativaFalha();
        }
    }

    private String topicoPara(String eventType) {
        return switch (eventType) {
            case "UsuarioCadastrado" -> "usuario.cadastrado";
            default -> throw new IllegalArgumentException("Tipo de evento desconhecido: " + eventType);
        };
    }
}
