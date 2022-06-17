package io.airbyte.integrations.destination.intempt.client.collection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;

public class CollectionService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CollectionService.class);

    private static final String BASE_URL =
            "https://api.staging.intempt.com/v1/";

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final HttpClient client;

    public CollectionService() {
        this.client = HttpClient.newHttpClient();
    }


    public HttpResponse<String> create(String body, String apiKey) throws IOException, InterruptedException, URISyntaxException {
        final String url = BASE_URL + "collections";
        final HttpRequest postRequest = HttpRequest.newBuilder(new URI(url))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .header("Authorization", "Bearer " + apiKey)
                .build();

        return client.send(postRequest, HttpResponse.BodyHandlers.ofString());
    }

    public JsonNode getByNameAndSourceId(String orgName, String apiKey, String sourceId, String name) throws IOException, InterruptedException, URISyntaxException {
        final String url = BASE_URL + orgName + "/collections/";
        LOGGER.info("making request to : {}", url);
        URI uri = new URIBuilder(url)
                .addParameter("sourceId", sourceId)
                .addParameter("name", name)
                .build();

        HttpRequest request = HttpRequest.newBuilder(uri)
                .GET()
                .header("Authorization","Bearer " + apiKey)
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        JsonNode collectionList = objectMapper.readTree(response.body()).get("_embedded").get("collections");

        for (JsonNode collection: collectionList) {
            if (collection.get("name").asText().equals(name) &&
                    collection.get("sourceId").asText().equals(sourceId)) {
                return collection;
            }
        }

        throw new IllegalArgumentException("Collection with given name and sourceId not found.");
    }

    private String convertToString(String name, String orgId, Schema schema, String sourceId) throws JsonProcessingException {
        HashMap<String, String> map = new HashMap<>();
        map.put("name", name);
        map.put("title", name);
        map.put("orgId", orgId);
        map.put("schema", schema.toString());
        map.put("sourceId", sourceId);

        return objectMapper.writeValueAsString(map);
    }
}
