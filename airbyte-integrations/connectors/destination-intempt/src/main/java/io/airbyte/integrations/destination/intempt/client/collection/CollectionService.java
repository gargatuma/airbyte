package io.airbyte.integrations.destination.intempt.client.collection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.integrations.destination.intempt.client.Service;
import org.apache.avro.Schema;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.HttpStatusCode;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CollectionService extends Service {

    private static final Logger LOGGER = LoggerFactory.getLogger(CollectionService.class);

    private static final String PATH = "/collections/";

    public HttpResponse<String> create(String orgName, String body, String apiKey)
            throws IOException, InterruptedException, URISyntaxException {
        final URI uri = createUri(orgName);
        return makePostRequest(apiKey, uri, body);
    }

    public HttpResponse<String> getByNameAndSourceId(String orgName, String apiKey, String sourceId, String name) throws IOException, InterruptedException, URISyntaxException {
        final URI uriGetByName = createUriGetByName(orgName, sourceId, name);
        return makeGetRequest(apiKey, uriGetByName);
    }

    public HttpResponse<String> update(String orgName, String apiKey, String collId, String body)
            throws IOException, InterruptedException, URISyntaxException {
        final URI uriPut = createUriPut(orgName, collId);
        return makePutRequest(apiKey, uriPut, body);
    }

    public boolean isEmpty(String body) throws JsonProcessingException {
        JsonNode collectionList = objectMapper.readTree(body).get("_embedded").get("collections");
        return collectionList.isEmpty();
    }

    public JsonNode extractCollectionList(String body) throws JsonProcessingException {
        LOGGER.info("Parsing collection list body: {}", body);
        return objectMapper.readTree(body).get("_embedded").get("collections").get(0);
    }

    public JsonNode extractCollection(String body) throws JsonProcessingException {
        LOGGER.info("Parsing collection body: {}", body);
        return objectMapper.readTree(body);
    }

    public void setDisplay(String orgName, String apiKey, String sourceId)
            throws IOException, URISyntaxException, InterruptedException {
        LOGGER.info("Set Display customers");
        final HttpResponse<String> customersResponse = getByNameAndSourceId(orgName, apiKey, sourceId, "customers");
        LOGGER.info("body: {}", customersResponse.body());
        final JsonNode collection = objectMapper.readTree(customersResponse.body()).get("_embedded").get("collections").get(0);

        LOGGER.info("Extracting");
        final JsonNode customers = collection.get("schema");

        LOGGER.info("Extracting 1");
        ArrayNode arrayNode = objectMapper.createArrayNode();
        List<JsonNode> list = new ArrayList<>();

        LOGGER.info("Extracting 2");
        customers.get("fields").forEach(field -> {
            LOGGER.info("Extracting inside {}", field);
            ((ObjectNode) field).set("display", BooleanNode.getTrue());
            LOGGER.info("Extracting inside after");
            list.add(field);
        });

        arrayNode.addAll(list);
        LOGGER.info("Setting display");
        ((ObjectNode) customers).set("fields", arrayNode);
        String collectionString = convertToString("customers", customers, sourceId);
        HttpResponse<String> update = update(orgName, apiKey, collection.get("id").asText(), collectionString);
        LOGGER.info("Completed setting display with status: {}", update.statusCode());
        if (update.statusCode() != HttpStatusCode.OK) {
            throw new RuntimeException(update.body());
        }
    }

    public String convertToString(String name, Schema schema, String sourceId) throws JsonProcessingException {
        LOGGER.info("Converting collection to string: {}, schema {} ", name, schema);
        return convertToString(name, objectMapper.readTree(schema.toString()), sourceId);
    }

    public String convertToString(String name, JsonNode schema, String sourceId) throws JsonProcessingException {
        LOGGER.info("Converting collection to string: {}, sourceId: {}, schema {} ", sourceId, name, schema);
        final Map<Object, Object> map = new HashMap<>();
        map.put("name", name);
        LOGGER.info("name");
        objectMapper.writeValueAsString(map);
        map.put("title", name);
        LOGGER.info("title");
        objectMapper.writeValueAsString(map);
        map.put("schema", schema);
        LOGGER.info("schema");
        objectMapper.writeValueAsString(map);
        map.put("sourceId", sourceId);
        LOGGER.info("sourceId");
        objectMapper.writeValueAsString(map);

        return objectMapper.writeValueAsString(map);
    }

    protected URI createUriGetByName(String orgName, String sourceId, String name) throws URISyntaxException {
        return new URIBuilder(HOST + orgName + PATH)
                .addParameter("sourceId", sourceId)
                .addParameter("name", name)
                .build();
    }

    protected URI createUriPut(String orgName, String collId) throws URISyntaxException {
        return new URI(HOST + orgName + PATH + collId);
    }

    @Override
    protected URI createUri(String orgName) throws URISyntaxException {
        return new URI(HOST + orgName + PATH);
    }
}
