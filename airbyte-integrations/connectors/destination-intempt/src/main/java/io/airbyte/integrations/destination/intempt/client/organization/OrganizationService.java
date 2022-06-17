package io.airbyte.integrations.destination.intempt.client.organization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class OrganizationService {

    private static final String BASE_URL =
            "https://api.staging.intempt.com/v1/";

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final HttpClient client;

    public OrganizationService() {
        this.client = HttpClient.newHttpClient();
    }

    public String getId(String name, String apiKey) throws IOException, InterruptedException, URISyntaxException {
        String url = BASE_URL + "organizations";

        HttpRequest request = HttpRequest.newBuilder(new URI(url))
                .header("Authorization","Bearer " + apiKey)
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        JsonNode jsonNode = objectMapper.readTree(response.body());

        JsonNode organizations = jsonNode.get("_embedded").get("organizations");

        for (JsonNode organization: organizations) {
            if (organization.get("name").asText().equals(name)) {
                return organization.get("id").asText();
            }
        }
        throw new IllegalArgumentException("API KEY does not have access to organization");
    }
}
