package io.airbyte.integrations.destination.intempt.client.source;

import io.airbyte.integrations.destination.intempt.client.Service;
import io.airbyte.protocol.models.AirbyteConnectionStatus;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;

public class SourceService extends Service {

    public AirbyteConnectionStatus checkById(String orgName, String apiKey, String sourceId)
            throws URISyntaxException, IOException, InterruptedException {
        final URI uri = createUriGet(orgName, sourceId);
        final HttpResponse<String> response = makeGetRequest(apiKey, uri);

        if (response.statusCode() != 200) {
            return new AirbyteConnectionStatus()
                    .withStatus(AirbyteConnectionStatus.Status.FAILED)
                    .withMessage(response.body());
        }
        return new AirbyteConnectionStatus()
                .withStatus(AirbyteConnectionStatus.Status.SUCCEEDED);
    }

    public String getType(String orgName, String apiKey, String sourceId)
            throws URISyntaxException, IOException, InterruptedException {
        final URI uri = createUriGet(orgName, sourceId);
        HttpResponse<String> response = makeGetRequest(apiKey, uri);
        return objectMapper.readTree(response.body()).get("type").asText();
    }

    protected URI createUriGet(String orgName, String sourceId) throws URISyntaxException {
        return new URI(HOST + orgName + "/sources/" + sourceId);
    }

    @Override
    protected URI createUri(String orgName) throws URISyntaxException {
        return new URI(HOST + orgName + "/sources/");
    }
}
