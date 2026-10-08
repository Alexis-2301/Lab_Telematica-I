package edu.unillanos.rabbitlab.publisher;

import com.google.gson.JsonObject;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
import edu.unillanos.rabbitlab.config.RabbitConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.concurrent.TimeoutException;

/**
 * Microservicio que publica eventos PaymentCreated en RabbitMQ.
 * No sabe quién los consumirá: solo conoce el exchange y la routing key.
 */
public class Publisher {

    private static final int TOTAL_MESSAGES = 5;
    private static final String[] STATUSES = {"APPROVED", "PENDING", "APPROVED", "REJECTED", "APPROVED"};

    public static void main(String[] args) {
        try (Connection connection = RabbitConfig.createConnectionFactory().newConnection("publisher");
             Channel channel = connection.createChannel()) {

            RabbitConfig.declareTopology(channel);
            // Confirmaciones: el broker avisa que recibió cada mensaje
            channel.confirmSelect();

            for (int i = 1; i <= TOTAL_MESSAGES; i++) {
                String json = buildEvent(i).toString();
                publish(channel, json);
                System.out.println("[Publisher] Enviado (" + i + "/" + TOTAL_MESSAGES + "): " + json);
            }

            System.out.println("[Publisher] Listo: " + TOTAL_MESSAGES + " mensajes publicados.");

        } catch (IOException e) {
            System.err.println("[Publisher] Error de comunicación con RabbitMQ: " + e.getMessage());
            System.err.println("             Verifica que el contenedor 'rabbitmq-lab' esté corriendo (docker ps).");
            System.exit(1);
        } catch (TimeoutException e) {
            System.err.println("[Publisher] Tiempo de espera agotado al conectar o confirmar: " + e.getMessage());
            System.exit(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("[Publisher] Ejecución interrumpida.");
            System.exit(1);
        }
    }

    /** Construye el evento con los campos mínimos pedidos por la guía. */
    private static JsonObject buildEvent(int number) {
        JsonObject event = new JsonObject();
        event.addProperty("eventType", "PaymentCreated");
        event.addProperty("id", String.format("PAY-%03d", number));
        event.addProperty("amount", 150000 + (number * 25000));
        event.addProperty("status", STATUSES[number - 1]);
        event.addProperty("createdAt", LocalDateTime.now().withNano(0).toString());
        return event;
    }

    /** Publica un mensaje JSON persistente y espera la confirmación del broker. */
    private static void publish(Channel channel, String json)
            throws IOException, InterruptedException, TimeoutException {
        AMQP.BasicProperties props = MessageProperties.PERSISTENT_TEXT_PLAIN.builder()
                .contentType("application/json")
                .build();

        channel.basicPublish(
                RabbitConfig.EXCHANGE,
                RabbitConfig.ROUTING_KEY,
                props,
                json.getBytes(StandardCharsets.UTF_8));

        channel.waitForConfirmsOrDie(5000);
    }
}