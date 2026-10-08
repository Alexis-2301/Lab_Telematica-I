package edu.unillanos.rabbitlab.config;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;

/**
 * Configuración compartida por el Publisher y el Consumer.
 * Centraliza los nombres definidos en la guía del laboratorio.
 */
public final class RabbitConfig {

    public static final String HOST = "localhost";
    public static final int PORT = 5672;
    public static final String USERNAME = "guest";
    public static final String PASSWORD = "guest";
    public static final String VIRTUAL_HOST = "/";

    public static final String EXCHANGE = "microservices.events";
    public static final String QUEUE = "payment.created.queue";
    public static final String ROUTING_KEY = "payment.created";

    private RabbitConfig() {
    }

    /** Crea la fábrica de conexiones con los datos del laboratorio. */
    public static ConnectionFactory createConnectionFactory() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setPort(PORT);
        factory.setUsername(USERNAME);
        factory.setPassword(PASSWORD);
        factory.setVirtualHost(VIRTUAL_HOST);
        factory.setConnectionTimeout(5000);
        return factory;
    }

    /**
     * Declara exchange, cola y binding. Es idempotente: si ya existen con la
     * misma configuración (como los creados con Postman), no pasa nada.
     */
    public static void declareTopology(Channel channel) throws IOException {
        channel.exchangeDeclare(EXCHANGE, "direct", true);
        channel.queueDeclare(QUEUE, true, false, false, null);
        channel.queueBind(QUEUE, EXCHANGE, ROUTING_KEY);
    }
}