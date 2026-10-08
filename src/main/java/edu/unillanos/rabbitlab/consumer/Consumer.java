package edu.unillanos.rabbitlab.consumer;

import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import edu.unillanos.rabbitlab.config.RabbitConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

/**
 * Microservicio que reacciona a los eventos PaymentCreated.
 * Lee de la cola, procesa el mensaje y lo confirma (ack) para que
 * RabbitMQ pueda eliminarlo.
 */
public class Consumer {

    private static final List<String> REQUIRED_FIELDS =
            List.of("eventType", "id", "status", "createdAt");

    public static void main(String[] args) {
        try {
            Connection connection = RabbitConfig.createConnectionFactory().newConnection("consumer");
            Channel channel = connection.createChannel();

            // Cierra la conexión limpiamente con Ctrl+C
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    if (connection.isOpen()) {
                        connection.close();
                    }
                } catch (IOException ignored) {
                    // se está cerrando la aplicación
                }
            }));

            RabbitConfig.declareTopology(channel);
            // Un mensaje a la vez: no recibe el siguiente hasta confirmar el actual
            channel.basicQos(1);

            System.out.println("[Consumer] Esperando mensajes en '" + RabbitConfig.QUEUE
                    + "'. Presiona Ctrl+C para salir.");

            channel.basicConsume(
                    RabbitConfig.QUEUE,
                    false, // autoAck desactivado: confirmamos manualmente
                    (consumerTag, delivery) -> handle(channel, delivery),
                    (CancelCallback) consumerTag ->
                            System.err.println("[Consumer] La cola fue cancelada por el broker."));

            new CountDownLatch(1).await(); // mantiene la aplicación activa

        } catch (IOException e) {
            System.err.println("[Consumer] Error de comunicación con RabbitMQ: " + e.getMessage());
            System.err.println("           Verifica que el contenedor 'rabbitmq-lab' esté corriendo (docker ps).");
            System.exit(1);
        } catch (TimeoutException e) {
            System.err.println("[Consumer] Tiempo de espera agotado al conectar: " + e.getMessage());
            System.exit(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Procesa un mensaje: valida, imprime y confirma. Si es inválido, lo rechaza. */
    private static void handle(Channel channel, Delivery delivery) throws IOException {
        long deliveryTag = delivery.getEnvelope().getDeliveryTag();
        String body = new String(delivery.getBody(), StandardCharsets.UTF_8);

        try {
            JsonObject event = JsonParser.parseString(body).getAsJsonObject();
            validate(event);
            process(event);
            channel.basicAck(deliveryTag, false); // OK: RabbitMQ lo elimina de la cola
        } catch (JsonParseException | IllegalStateException | IllegalArgumentException e) {
            System.err.println("[Consumer] Mensaje inválido descartado: " + body);
            System.err.println("           Motivo: " + e.getMessage());
            // requeue=false: evita un ciclo infinito con un mensaje que nunca será válido
            channel.basicNack(deliveryTag, false, false);
        }
    }

    private static void validate(JsonObject event) {
        for (String field : REQUIRED_FIELDS) {
            if (!event.has(field)) {
                throw new IllegalArgumentException("falta el campo obligatorio '" + field + "'");
            }
        }
    }

    /** Lógica de negocio: aquí solo se imprime el evento recibido. */
    private static void process(JsonObject event) {
        System.out.println("[Consumer] Recibido -> " + event.get("eventType").getAsString()
                + " | id=" + event.get("id").getAsString()
                + " | status=" + event.get("status").getAsString()
                + " | createdAt=" + event.get("createdAt").getAsString());
        System.out.println("           JSON completo: " + event);
    }
}