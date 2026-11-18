package com.ejemplo.fraude.controller;

import com.ejemplo.fraude.model.Transaccion;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/transacciones")
public class TransaccionController {

    private final KafkaTemplate<String, Transaccion> kafkaTemplate;

    public TransaccionController(KafkaTemplate<String, Transaccion> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    public String registrarTransaccion(@RequestBody Transaccion tx) {
        // La clave (tx.usuarioId()) asegura que eventos del mismo usuario vayan a la misma partición
        kafkaTemplate.send("transacciones-bancarias", tx.usuarioId(), tx);
        return "Transacción enviada al topic para usuario: " + tx.usuarioId();
    }
}