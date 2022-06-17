package io.airbyte.integrations.destination.intempt.client.source;

import io.airbyte.protocol.models.AirbyteConnectionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class SourceService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SourceService.class);

    private static final String BASE_URL =
            "https://api.staging.intempt.com/v1/";

    private final HttpClient client;

    public SourceService() {
        this.client = HttpClient.newHttpClient();
    }

    public AirbyteConnectionStatus exists(String orgName, String apiKey, String sourceId) throws URISyntaxException, IOException, InterruptedException {
        final String url = BASE_URL + orgName + "/sources/" + sourceId;
        LOGGER.info("generated url: {}", url);
        HttpRequest request = HttpRequest.newBuilder(new URI(url))
                .GET()
                .header("Authorization","Bearer " + apiKey)
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            return new AirbyteConnectionStatus()
                    .withStatus(AirbyteConnectionStatus.Status.FAILED)
                    .withMessage(response.body());
        }
        return new AirbyteConnectionStatus()
                .withStatus(AirbyteConnectionStatus.Status.SUCCEEDED);
    }
}
