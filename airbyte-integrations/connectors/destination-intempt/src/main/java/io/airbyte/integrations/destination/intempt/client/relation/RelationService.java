package io.airbyte.integrations.destination.intempt.client.relation;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.airbyte.integrations.destination.intempt.client.Service;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;

public class RelationService extends Service {

    public HttpResponse<String> create(String orgName, String apiKey, String name, String refId, RelationType type)
            throws IOException, InterruptedException, URISyntaxException {
        final String relation = createRelation(name, type, refId);
        final URI uri = createUri(orgName);
        return makePostRequest(apiKey, uri, relation);
    }

    private String createRelation(String name, RelationType type, String refId) throws JsonProcessingException {
        Map<Object, Object> identifier = new HashMap<>();
        identifier.put("name", name);
        identifier.put("identifierId", refId);
        identifier.put("type", type.getValue());

        return objectMapper.writeValueAsString(identifier);
    }

    @Override
    protected URI createUri(String orgName) throws URISyntaxException {
        return new URI(HOST + orgName + "/relations");
    }
}
