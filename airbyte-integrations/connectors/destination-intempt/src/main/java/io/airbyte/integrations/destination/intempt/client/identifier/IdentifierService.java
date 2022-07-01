package io.airbyte.integrations.destination.intempt.client.identifier;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.integrations.destination.intempt.client.Service;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;

public class IdentifierService extends Service {

    private static final Logger LOGGER = LoggerFactory.getLogger(IdentifierService.class);

    private static final String PATH = "/identifiers/";

    public HttpResponse<String> createProfileId(String orgName, String apiKey,
                                                JsonNode collection, String name, Schema schema)
            throws IOException, InterruptedException, URISyntaxException {
        final String idName = StringUtils.capitalize(name);
        final String stringId = createIdentifier(collection, idName, IdentifierType.PROFILE, schema, null);

        final URI uri = createUri(orgName);
        return makePostRequest(apiKey, uri, stringId);
    }

    public HttpResponse<String> createPrimaryId(String orgName, String apiKey,
                                                JsonNode collection, String name, Schema schema)
            throws IOException, InterruptedException, URISyntaxException {
        final String stringId = createIdentifier(collection, name, IdentifierType.PRIMARY, schema, null);
        final URI uri = createUri(orgName);
        return makePostRequest(apiKey, uri, stringId);
    }

    public HttpResponse<String> createForeignId(String orgName, String apiKey, JsonNode collection,
                                                Schema schema, String name, String refId)
            throws IOException, InterruptedException, URISyntaxException {
        final String stringId = createIdentifier(collection, name, IdentifierType.FOREIGN, schema, refId);
        final URI uri = createUri(orgName);
        return makePostRequest(apiKey, uri, stringId);
    }

    public HttpResponse<String> getByCollId(String orgName, String apiKey, String collId)
            throws IOException, InterruptedException, URISyntaxException {
        final URI uriGetByCollId = createUriGetByCollId(orgName, collId);
        return makeGetRequest(apiKey, uriGetByCollId);
    }

    public JsonNode getPrimaryOrNull(String body) throws JsonProcessingException {
        return extractIdentifier(body, IdentifierType.PRIMARY);
    }

    public JsonNode getProfileOrNull(String body) throws JsonProcessingException {
        return extractIdentifier(body, IdentifierType.PROFILE);
    }

    public JsonNode getForeignOrNull(String body) throws JsonProcessingException {
        return extractIdentifier(body, IdentifierType.FOREIGN);
    }

    public JsonNode extractIdentifier(String body, IdentifierType type) throws JsonProcessingException {
        LOGGER.info("Parsing collection list body: {}", body);
        JsonNode identifierList = objectMapper.readTree(body).get("_embedded").get("identifiers");

        for (JsonNode identifier : identifierList) {
            if (identifier.get("type").asText().equals(type.getValue())) {
                return identifier;
            }
        }

        return objectMapper.nullNode();
    }

    private String createIdentifier(JsonNode coll, String name, IdentifierType type, Schema schema, String refId)
            throws JsonProcessingException {
        final Map<Object, Object> identifier = new HashMap<>();
        final JsonNode jsonNode = objectMapper.readTree(schema.toString());
        identifier.put("collId", coll.get("id"));
        identifier.put("type", type.getValue());
        identifier.put("name", name);
        identifier.put("schema", jsonNode);

        if (refId != null) {
            LOGGER.info("putting refId");
            identifier.put("referenceId", refId);
        }

        return objectMapper.writeValueAsString(identifier);
    }

    @Override
    protected URI createUri(String orgName) throws URISyntaxException {
        return new URI(HOST + orgName + PATH);
    }

    private URI createUriGetByCollId(String orgName, String collId) throws URISyntaxException {
        return new URIBuilder(HOST + orgName + PATH)
                .addParameter("collId", collId)
                .build();
    }
}
