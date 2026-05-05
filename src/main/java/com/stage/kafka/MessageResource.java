package com.stage.kafka;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;

@Path("/messages")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class MessageResource {

    @Channel("messages-out")
    Emitter<String> emitter;

    @POST
    public CompletionStage<Response> send(MessageRequest request) {
        String key = request.key != null ? request.key : "default-key";
        String payload = request.message != null ? request.message : "";

        CompletableFuture<Response> future = new CompletableFuture<>();

        OutgoingKafkaRecordMetadata<String> metadata = OutgoingKafkaRecordMetadata
                .<String>builder()
                .withKey(key)
                .build();

        Message<String> message = Message.of(payload)
                .addMetadata(metadata)
                .withAck(() -> {
                    future.complete(Response.accepted()
                            .entity(new ApiResponse("Message envoyé à Kafka"))
                            .build());
                    return CompletableFuture.completedFuture(null);
                })
                .withNack(throwable -> {
                    future.completeExceptionally(throwable);
                    return CompletableFuture.completedFuture(null);
                });

        emitter.send(message);
        return future;
    }

    public static class ApiResponse {
        public String status;

        public ApiResponse(String status) {
            this.status = status;
        }
    }
}