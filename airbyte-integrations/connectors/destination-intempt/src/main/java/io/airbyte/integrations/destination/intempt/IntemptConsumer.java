package io.airbyte.integrations.destination.intempt;

import io.airbyte.integrations.base.FailureTrackingAirbyteMessageConsumer;
import io.airbyte.integrations.destination.intempt.client.push.PushService;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

@AllArgsConstructor
public class IntemptConsumer extends FailureTrackingAirbyteMessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(IntemptConsumer.class);

    private final PushService pushService = new PushService();

    private final String orgName;

    private final String apiKey;

    private final Map<String, String> collectionId;

    @Override
    protected void startTracked() throws Exception {
        // determine what to do
        LOGGER.info("starting intempt consumer");
    }

    @Override
    protected void acceptTracked(AirbyteMessage msg) throws Exception {
        LOGGER.info("msg recieved {}", msg);
        if (msg.getType() == AirbyteMessage.Type.RECORD) {
            final AirbyteRecordMessage recordMsg = msg.getRecord();
            pushService.pushData(orgName, recordMsg.toString(), collectionId.get(recordMsg.getStream()), apiKey);
            LOGGER.info("data pushed succesfully");
        }
    }

    @Override
    protected void close(boolean hasFailed) throws Exception {
        // determine what to do
        LOGGER.info("closing intempt consumer");
    }
}
