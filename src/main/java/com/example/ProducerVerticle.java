package com.example;

import io.vertx.core.AbstractVerticle;
import io.vertx.ext.amqp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerVerticle extends AbstractVerticle {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerVerticle.class);

    private AmqpClient client;

    @Override
    public void start() throws Exception {

        AmqpClientOptions options = new AmqpClientOptions()
                .setHost("localhost")
                .setPort(5672);
        // Create a client using its own internal Vert.x instance.
        //AmqpClient client = AmqpClient.create(options);

        // USe an explicit Vert.x instance.
        client = AmqpClient.create(vertx, options);

        client.connect(ar -> {
            if (ar.failed()) {
                LOGGER.info("Unable to connect to the broker");
            } else {
                LOGGER.info("Connection succeeded");
                AmqpConnection connection = ar.result();
                connection.createSender("my-queue", done -> {
                    if (done.failed()) {
                        LOGGER.info("Unable to create a sender");
                    } else {
                        LOGGER.info("Sender created");
                        AmqpSender sender = done.result();
                        vertx.setPeriodic(5000, x -> {
                            LOGGER.info("Sending Hello to consumer");
                            sender.send(AmqpMessage.create().withBody("Hello from Producer").build());
                        });
                    }
                });
            }
        });

        super.start();
    }

    @Override
    public void stop() throws Exception {

        client.close(x -> {
            if (x.succeeded()) {
                LOGGER.info("AmqpClient closed successfully");
            }
        });

        super.stop();
    }
}
