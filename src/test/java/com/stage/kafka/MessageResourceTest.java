package com.stage.kafka;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.equalTo;

@QuarkusTest
public class MessageResourceTest {

    @Test
    public void testSendMessage() {
        given()
            .contentType(ContentType.JSON)
            .body("{\"key\": \"test-key\", \"message\": \"hello kafka\"}")
        .when()
            .post("/messages")
        .then()
            .statusCode(202)
            .body("status", equalTo("Message envoyé à Kafka"));
    }
}