package io.airbyte.integrations.destination.intempt.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpException;
import software.amazon.awssdk.http.HttpStatusCode;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

public abstract class Service {

    public static final String HOST =
            "https://api.staging.intempt.com/v1/";

    protected final ObjectMapper objectMapper = new ObjectMapper();

    protected final HttpClient client = HttpClient.newHttpClient();


    public void isOK(HttpResponse<String> response) throws HttpException {
        if (response.statusCode() != HttpStatusCode.OK) {
            throw new HttpException(response.body() + "\nstatus-code: " + response.statusCode());
        }
    }

    protected HttpResponse<String> makePostRequest(String apiKey, URI uri, String body)
            throws IOException, InterruptedException {
        final HttpRequest postRequest = HttpRequest.newBuilder(uri)
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .header("Authorization", "Bearer " + apiKey)
                .header("Content-Type", "application/json")
                .build();

        return client.send(postRequest, HttpResponse.BodyHandlers.ofString());
    }

    protected HttpResponse<String> makeGetRequest(String apiKey, URI uri) throws IOException, InterruptedException {
        final HttpRequest postRequest = HttpRequest.newBuilder(uri)
                .GET()
                .header("Authorization", "Bearer " + apiKey)
                .build();

        return client.send(postRequest, HttpResponse.BodyHandlers.ofString());
    }

    protected HttpResponse<String> makePutRequest(String apiKey, URI uri, String body) throws IOException, InterruptedException {
        final HttpRequest postRequest = HttpRequest.newBuilder(uri)
                .PUT(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .header("Authorization", "Bearer " + apiKey)
                .header("Content-Type", "application/json")
                .build();

        return client.send(postRequest, HttpResponse.BodyHandlers.ofString());
    }

    protected abstract URI createUri(String orgName) throws URISyntaxException;
}
