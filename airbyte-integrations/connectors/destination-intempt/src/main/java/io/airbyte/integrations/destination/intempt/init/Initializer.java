package io.airbyte.integrations.destination.intempt.init;

import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;

import java.util.Map;

public interface Initializer {


    Map<String, String> init(String orgName, String apiKey, String sourceId,
                             ConfiguredAirbyteCatalog catalog, String sourceType);

}
