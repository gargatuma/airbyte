/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.intempt;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.integrations.BaseConnector;
import io.airbyte.integrations.base.AirbyteMessageConsumer;
import io.airbyte.integrations.base.Destination;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.destination.intempt.client.source.SourceService;
import io.airbyte.integrations.destination.intempt.init.Initializer;
import io.airbyte.integrations.destination.intempt.init.StripeInitializer;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Consumer;

public class IntemptDestination extends BaseConnector implements Destination {

  private final SourceService sourceService = new SourceService();

  private static final Logger LOGGER = LoggerFactory.getLogger(IntemptDestination.class);

  private final Map<String, Initializer> initializerMap = Map.of(
          "api", new StripeInitializer());

  public static void main(String[] args) throws Exception {
    LOGGER.info("starting destination: {}", IntemptDestination.class);
    new IntegrationRunner(new IntemptDestination()).run(args);
    LOGGER.info("completed destination: {}", IntemptDestination.class);
  }

  @Override
  public AirbyteConnectionStatus check(JsonNode config) {
    try {
      final String apiKey = config.get("api_key").asText();
      final String orgName = config.get("org_name").asText();
      final String sourceId = config.get("source_id").asText();

      return sourceService.checkById(orgName, apiKey, sourceId);
    } catch (Exception e) {
      return new AirbyteConnectionStatus()
              .withStatus(AirbyteConnectionStatus.Status.FAILED)
              .withMessage(e.getMessage());
    }
  }

  @Override
  public AirbyteMessageConsumer getConsumer(JsonNode config,
                                            ConfiguredAirbyteCatalog configuredCatalog,
                                            Consumer<AirbyteMessage> outputRecordCollector) {

    LOGGER.info("Extracting secrets from config");
    final String apiKey = config.get("api_key").asText();
    final String orgName = config.get("org_name").asText();
    final String sourceId = config.get("source_id").asText();

    try {
      final String sourceType = sourceService.getType(orgName, apiKey, sourceId);
      LOGGER.info("SourceType value: {}", sourceType);
      final Map<String, String> collectionId = initializerMap.get(sourceType)
              .init(orgName, apiKey, sourceId, configuredCatalog, sourceType);

      return new IntemptConsumer(orgName, apiKey, collectionId);
    } catch (Exception e) {
      throw new RuntimeException();
    }
  }
}
