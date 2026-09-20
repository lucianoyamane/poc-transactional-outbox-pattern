package com.lucianoyamane.outbox.usuario;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;

public record UsuarioCadastroRequest(
        @NotBlank String nome,
        @NotBlank @Email String email
) {
}
