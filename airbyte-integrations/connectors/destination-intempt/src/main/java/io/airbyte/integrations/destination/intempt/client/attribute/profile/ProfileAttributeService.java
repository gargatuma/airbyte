package io.airbyte.integrations.destination.intempt.client.attribute.profile;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.integrations.destination.intempt.client.Service;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;

public class ProfileAttributeService extends Service {
    private static final String PATH = "/profile-attributes";

    public HttpResponse<String> create(String orgName, String apiKey, JsonNode collection, String fieldName) throws IOException, InterruptedException, URISyntaxException {
        final String profileAttribute = createProfileAttribute(collection, fieldName);
        final URI uri = createUri(orgName);
        return makePostRequest(apiKey, uri, profileAttribute);
    }

    private String createProfileAttribute(JsonNode coll, String fieldName) throws JsonProcessingException {
        Map<Object, Object> identifier = new HashMap<>();
        identifier.put("orgId", coll.get("orgId"));
        identifier.put("sourceId", coll.get("sourceId"));
        identifier.put("collId", coll.get("id"));
        identifier.put("name", fieldName);
        identifier.put("schema", coll.get("schema"));

        return objectMapper.writeValueAsString(identifier);
    }

    @Override
    protected URI createUri(String orgName) throws URISyntaxException {
        return new URI(HOST + orgName + PATH);
    }
}
