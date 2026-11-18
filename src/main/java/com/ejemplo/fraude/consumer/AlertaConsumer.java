package com.ejemplo.fraude.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AlertaConsumer {

    @KafkaListener(topics = "alertas-fraude", groupId = "grupo-notificaciones-fraude")
    public void procesarAlerta(String mensajeAlerta) {
        System.out.println("🚨 [BLOQUEO PREVENTIVO] -> " + mensajeAlerta);
    }
}
