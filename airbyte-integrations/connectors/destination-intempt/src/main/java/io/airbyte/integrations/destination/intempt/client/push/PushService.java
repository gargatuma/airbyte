package io.airbyte.integrations.destination.intempt.client.push;

import io.airbyte.integrations.destination.intempt.client.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;

public class PushService extends Service {

    private static final Logger LOGGER = LoggerFactory.getLogger(PushService.class);

    public HttpResponse<String> pushData(String orgName, String body, String collectionId, String apiKey) throws IOException, InterruptedException, URISyntaxException {
        final URI uri = createUri(orgName, collectionId);

        return makePostRequest(apiKey, uri, body);
    }

    protected URI createUri(String orgName, String collId) throws URISyntaxException {
        return new URI(HOST + orgName + "/collections/" + collId + "/data");
    }

    @Override
    protected URI createUri(String orgName) throws URISyntaxException {
        return null;
    }
}
