package com.lucianoyamane.outbox.notificacao;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NotificacaoListenerTest {

    private final NotificacaoListener listener = new NotificacaoListener();

    @Test
    void armazenaMensagemRecebidaParaInspecao() {
        listener.aoReceberUsuarioCadastrado("{\"nome\":\"Joao\"}");

        assertThat(listener.getMensagensRecebidas()).containsExactly("{\"nome\":\"Joao\"}");
    }
}
