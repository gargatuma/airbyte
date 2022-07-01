package io.airbyte.integrations.destination.intempt.client.identifier;

import com.fasterxml.jackson.annotation.JsonValue;

public enum IdentifierType {

    PRIMARY, FOREIGN, USER, PROFILE, ACCOUNT;

    public String getValue() {
        return this.name().toLowerCase();
    }
}
