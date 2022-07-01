package io.airbyte.integrations.destination.intempt.init;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.CaseFormat;
import io.airbyte.integrations.destination.intempt.client.attribute.profile.ProfileAttributeService;
import io.airbyte.integrations.destination.intempt.client.collection.CollectionService;
import io.airbyte.integrations.destination.intempt.client.identifier.IdentifierService;
import io.airbyte.integrations.destination.intempt.client.relation.RelationService;
import io.airbyte.integrations.destination.intempt.client.relation.RelationType;
import io.airbyte.integrations.destination.s3.avro.JsonToAvroSchemaConverter;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.HttpStatusCode;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;

public class StripeInitializer implements Initializer{

    private static final Logger LOGGER = LoggerFactory.getLogger(StripeInitializer.class);

    private static final String NAMESPACE = "com.intempt.data.stripe";

    private final IdentifierService identifierService = new IdentifierService();

    private final RelationService relationService = new RelationService();

    private final CollectionService collectionService = new CollectionService();

    private final ProfileAttributeService profileAttributeService = new ProfileAttributeService();

    private final JsonToAvroSchemaConverter schemaConverter = new JsonToAvroSchemaConverter();

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Map<String, String> init(String orgName, String apiKey, String sourceId,
                                    ConfiguredAirbyteCatalog catalog, String sourceType) {

        LOGGER.info("Stripe init started");
        final Map<String, JsonNode> collections = new HashMap<>();
        final Map<String, JsonNode> identifiers = new HashMap<>();

        LOGGER.info("Stripe init starting collection and id generation");
        catalog.getStreams().forEach(airbyteStream -> {
                    try {
                        final AirbyteStream stream = airbyteStream.getStream();
                        JsonNode collection = createCollection(orgName, apiKey, sourceId, stream);
                        JsonNode identifier = createPrimaryId(orgName, apiKey, collection);
                        identifiers.put(collection.get("name").asText(), identifier);
                        collections.put(collection.get("name").asText(), collection);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
        });

        // Get all streams that contain customer field. Add foreign ids and relations
        LOGGER.info("Stripe init starting foreign id and relation");
        catalog.getStreams().stream()
                .filter(airbyteStream -> airbyteStream.getStream().getJsonSchema().get("properties").has("customer"))
                .forEach(airbyteStream -> {
                    try {
                        final String name = airbyteStream.getStream().getName();
                        LOGGER.info("starting foreign id and relation for: {}", name);
                        createForeignIdAndRelations(orgName, apiKey, collections.get(name),
                                identifiers.get("customers").get("id").asText(), name);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });


        try {
            LOGGER.info("starting profile id init");
            createProfileId(orgName, apiKey, collections.get("customers"));
            createProfileAttribute(orgName, apiKey, collections.get("customers"));
            final Map<String, String> collectionId = new HashMap<>();
            collections.entrySet().forEach(set -> {
                collectionId.put(set.getKey(), set.getValue().get("id").asText());
            });
            collectionService.setDisplay(orgName, apiKey, sourceId);
            LOGGER.info("returning collectionsID");
            return collectionId;
        } catch (Exception e) {
            return null;
        }
    }

    private void createProfileAttribute(String orgName, String apiKey, JsonNode collection) throws IOException, URISyntaxException, InterruptedException {
        HttpResponse<String> email = profileAttributeService.create(orgName, apiKey, collection, "email");
        if (email.statusCode() != HttpStatusCode.OK) {
            throw new RuntimeException("Failed to create profile attribute email");
        }

        HttpResponse<String> phone = profileAttributeService.create(orgName, apiKey, collection, "phone");
        if (phone.statusCode() != HttpStatusCode.OK) {
            throw new RuntimeException("Failed to create profile attribute phone");
        }

        HttpResponse<String> name = profileAttributeService.create(orgName, apiKey, collection, "name");

        if (name.statusCode() != HttpStatusCode.OK) {
            throw new RuntimeException("Failed to create profile attribute name");
        }
    }

    private void createProfileId(String orgName, String apiKey, JsonNode collection) throws IOException, URISyntaxException, InterruptedException {
        HttpResponse<String> profileId = identifierService.createProfileId(orgName, apiKey, collection,
                "CustomerId", primaryIdSchema("CustomerId"));

        if (profileId.statusCode() != HttpStatusCode.OK) {
            throw new RuntimeException(profileId.body());
        }
    }

    private void createForeignIdAndRelations(String orgName, String apiKey, JsonNode collection, String id, String name) throws IOException, URISyntaxException, InterruptedException {
        LOGGER.info("is identifier exist: {}", collection);
        HttpResponse<String> getByCollId = identifierService.getByCollId(orgName, apiKey, collection.get("id").asText());
        LOGGER.info("By name and source id status-code: {}", getByCollId.statusCode());
        if (getByCollId.statusCode() != HttpStatusCode.OK) {
            LOGGER.info("Throwing error for status-code: {}", getByCollId.statusCode());
            throw new RuntimeException(getByCollId.body());
        }

        JsonNode foreignOrNull = identifierService.getForeignOrNull(getByCollId.body());

        if (foreignOrNull.isNull()) {
            final HttpResponse<String> foreignId = identifierService.createForeignId(orgName, apiKey, collection,
                    foreignIdSchema(), name, id);
            LOGGER.info("foreignId statuscode: {}", foreignId.statusCode());
            if (foreignId.statusCode() != HttpStatusCode.OK) {
                throw new RuntimeException(foreignId.body());
            }
            final JsonNode idNode = objectMapper.readTree(foreignId.body());
            final HttpResponse<String> response = relationService.create(
                    orgName, apiKey, name, idNode.get("id").asText(), RelationType.MANY_TO_ONE);
            LOGGER.info("relation statuscode: {}", response.statusCode());

            if (response.statusCode() != HttpStatusCode.OK) {
                throw new RuntimeException(foreignId.body());
            }
        }
    }

    private JsonNode createPrimaryId(String orgName, String apiKey, JsonNode collection) throws IOException, URISyntaxException, InterruptedException {
        LOGGER.info("idInit: {}", collection);
        LOGGER.info("is identifier exist: {}", collection);
        HttpResponse<String> id = identifierService.getByCollId(orgName, apiKey, collection.get("id").asText());
        LOGGER.info("By name and source id status-code: {}", id.statusCode());
        if (id.statusCode() != HttpStatusCode.OK) {
            LOGGER.info("Throwing error for status-code: {}", id.statusCode());
            throw new RuntimeException(id.body());
        }

        JsonNode primaryOrNull = identifierService.getPrimaryOrNull(id.body());

        if (primaryOrNull.isNull()) {
            final String name = CaseFormat.LOWER_HYPHEN.to(CaseFormat.UPPER_CAMEL, collection.get("name").asText()).concat("Id");
            final Schema schema = primaryIdSchema(name);
            LOGGER.info("Creating primary name: {}, schema: {}", name, schema);
            final HttpResponse<String> primaryId =
                    identifierService.createPrimaryId(orgName, apiKey, collection, name, schema);
            LOGGER.info("primary id statuscode: {}", primaryId.statusCode());
            if (primaryId.statusCode() != HttpStatusCode.OK) {
                LOGGER.info("throwing runtime exception: {}", primaryId.body());
                throw new RuntimeException(primaryId.body());
            }
            return objectMapper.readTree(primaryId.body());
        }
        return primaryOrNull;
    }

    private JsonNode createCollection(String orgName, String apiKey, String sourceId, AirbyteStream stream)
            throws IOException, URISyntaxException, InterruptedException {
        LOGGER.info("Starting collection init for stream {}", stream);
        final HttpResponse<String> byNameAndSourceId = collectionService.getByNameAndSourceId(
                orgName, apiKey, sourceId, stream.getName());

        LOGGER.info("By name and source id statuscode: {}", byNameAndSourceId.statusCode());
        if (byNameAndSourceId.statusCode() != HttpStatusCode.OK) {
            LOGGER.info("Throwing error for statuscode: {}", byNameAndSourceId.statusCode());
            throw new RuntimeException(byNameAndSourceId.body());
        }

        LOGGER.info("Checking if collections exists: {}", byNameAndSourceId.body());
        if (collectionService.isEmpty(byNameAndSourceId.body())) {
            LOGGER.info("Collections does not exists! Converting avro schema: ");
            final Schema schema = schemaConverter.getAvroSchema(
                    stream.getJsonSchema(), stream.getName(), NAMESPACE);

            final String collection = collectionService.convertToString(
                    stream.getName(), schema, sourceId);
            final HttpResponse<String> postResponse = collectionService.create(
                    orgName, collection, apiKey);

            LOGGER.info("Collection POST statuscode: {}", postResponse.statusCode());
            if (postResponse.statusCode() != HttpStatusCode.OK) {
                LOGGER.info("Throwing Illegalexception: {}", postResponse.body());
                throw new IllegalArgumentException(postResponse.body());
            }
            return collectionService.extractCollection(postResponse.body());
        }

        return collectionService.extractCollectionList(byNameAndSourceId.body());
    }

    private Schema primaryIdSchema(String name) {
        return SchemaBuilder.record(name)
                .namespace(NAMESPACE)
                .fields()
                .optionalString("id")
                .endRecord();
    }

    private Schema foreignIdSchema() {
        return SchemaBuilder.record("customers_id")
                .namespace(NAMESPACE)
                .fields()
                .optionalString("customer")
                .endRecord();
    }
}
