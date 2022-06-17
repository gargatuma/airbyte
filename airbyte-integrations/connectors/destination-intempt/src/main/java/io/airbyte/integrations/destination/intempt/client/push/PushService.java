package io.airbyte.integrations.destination.intempt.client.push;

import software.amazon.awssdk.http.HttpStatusCode;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class PushService {

    private static final String BASE_URL = "https://api.staging.intempt.com/v1/";

    private final HttpClient client;

    public PushService() {
        this.client = HttpClient.newHttpClient();
    }

    public void pushData(String body, String collectionId, String apiKey) throws IOException, InterruptedException, URISyntaxException {
        final String url = BASE_URL + "collections/" + collectionId + "/data";

        HttpRequest request = HttpRequest.newBuilder(new URI(url))
                .header("Authorization","Bearer " + apiKey)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != HttpStatusCode.CREATED) {
            throw new IllegalArgumentException(response.body());
        }
    }
}
