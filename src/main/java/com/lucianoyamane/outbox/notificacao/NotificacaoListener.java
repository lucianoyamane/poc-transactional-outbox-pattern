package com.lucianoyamane.outbox.notificacao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Component
public class NotificacaoListener {

    private static final Logger log = LoggerFactory.getLogger(NotificacaoListener.class);

    private final List<String> mensagensRecebidas = Collections.synchronizedList(new ArrayList<>());

    @KafkaListener(topics = "usuario.cadastrado", groupId = "notificacao-service")
    public void aoReceberUsuarioCadastrado(String payload) {
        log.info("[EMAIL BOAS-VINDAS] Simulando envio de e-mail para o evento recebido: {}", payload);
        mensagensRecebidas.add(payload);
    }

    public List<String> getMensagensRecebidas() {
        return List.copyOf(mensagensRecebidas);
    }
}
