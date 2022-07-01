package io.airbyte.integrations.destination.intempt.client.relation;

import com.fasterxml.jackson.annotation.JsonValue;

public enum RelationType {
    ONE_TO_MANY, MANY_TO_ONE;

    @JsonValue
    public String getValue() {
        return this.name().toLowerCase();
    }
}
