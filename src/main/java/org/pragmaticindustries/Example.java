package org.pragmaticindustries;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.plc4x.java.PlcDriverManager;
import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.api.exceptions.PlcConnectionException;
import org.apache.plc4x.java.api.messages.PlcSubscriptionEvent;
import org.apache.plc4x.java.api.model.PlcConsumerRegistration;
import org.apache.plc4x.java.api.model.PlcSubscriptionHandle;
import org.apache.plc4x.java.api.types.PlcResponseCode;
import org.apache.plc4x.java.base.messages.items.BaseDefaultFieldItem;
import org.apache.plc4x.java.base.messages.items.DefaultIntegerFieldItem;
import org.apache.plc4x.java.mock.MockDevice;
import org.apache.plc4x.java.mock.PlcMockConnection;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.client.DittoClients;
import org.eclipse.ditto.client.configuration.BasicAuthenticationConfiguration;
import org.eclipse.ditto.client.configuration.MessagingConfiguration;
import org.eclipse.ditto.client.configuration.WebSocketMessagingConfiguration;
import org.eclipse.ditto.client.messaging.AuthenticationProviders;
import org.eclipse.ditto.client.messaging.internal.WebSocketMessagingProvider;
import org.eclipse.ditto.model.things.Feature;
import org.eclipse.ditto.model.things.Features;
import org.eclipse.ditto.model.things.Thing;
import org.eclipse.ditto.model.things.ThingId;
import org.eclipse.ditto.signals.commands.things.exceptions.ThingNotAccessibleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Very simple Example that shows how to use Apaches PLC4X together with
 * Eclipse Ditto.
 */
public class Example {

    private static final Logger logger = LoggerFactory.getLogger(Example.class);

    private static final String DITTO_API_ENDPOINT = "ws://localhost:8080/ws/2";
    private static final String DITTO_USER = "ditto";
    private static final String DITTO_PASSWORD = "ditto";
    private static final String THING_NAME = "org.pragmaticindustries:my-plc";

    // Mocked
    private static final String PLC4X_FIELD_NAME = "pressure";
    private static final String PLC4X_PLC_ADDRESS = "mock:plc";
    private static final String PLC4X_FIELD_ADDRESS = "%DB:xxx";

//    // Real Siemens Device
//    private static final String PLC4X_FIELD_NAME = "pressure";
//    private static final String PLC4X_PLC_ADDRESS = "s7://192.168.167.210/1/1";
//    private static final String PLC4X_FIELD_ADDRESS = "%DB555.DBD0:DINT";

    public static void main(String[] args) throws ExecutionException, InterruptedException, PlcConnectionException {
        MessagingConfiguration configuration = WebSocketMessagingConfiguration.newBuilder()
            .endpoint(DITTO_API_ENDPOINT)
            .build();
        WebSocketMessagingProvider provider = WebSocketMessagingProvider.newInstance(configuration,
            AuthenticationProviders
                .basic(BasicAuthenticationConfiguration.newBuilder()
                    .username(DITTO_USER)
                    .password(DITTO_PASSWORD)
                    .build()),
            Executors.newFixedThreadPool(4));
        DittoClient client = DittoClients.newInstance(provider);

        // Check if the Thing already exists
        try {
            client.twin().forId(ThingId.of(THING_NAME)).retrieve().get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ThingNotAccessibleException) {
                // Not existing, create
                logger.info("Digital Twin not found, creating new...");
                Thing thing = Thing.newBuilder()
                    .setId(ThingId.of(THING_NAME))
                    .setFeature("live-data")
                    .build();
                client.twin().create(thing).get();
            }
        }

        // Check if the feature exists
        boolean present = client.twin().forId(ThingId.of(THING_NAME)).retrieve().get()
            .getFeatures().map(features -> features.getFeature("live-data")).isPresent();
        if (!present) {
            logger.info("feature not present, adding feature...");
            client.twin().forId(ThingId.of(THING_NAME)).setFeatures(Features.newBuilder().set(Feature.newBuilder().withId("live-data").build()).build());
        }
        // Put Attribute in again
        client.twin().forId(ThingId.of(THING_NAME)).putAttribute("plc-address", PLC4X_PLC_ADDRESS).get();

        // Prepare the Mock
        PlcDriverManager plcDriverManager = new PlcDriverManager();
        if (PLC4X_PLC_ADDRESS.startsWith("mock:")) {
            setupMock(plcDriverManager);
        }

        // Now start the loop
        try (PlcConnection connection = plcDriverManager.getConnection(PLC4X_PLC_ADDRESS)) {
            for (int i = 1; i <= 100_000; i++) {
                int value = connection.readRequestBuilder().addItem("item1", PLC4X_FIELD_ADDRESS).build().execute().get().getInteger("item1");
                logger.debug("Got value {} from PLC, sending", value);
                client.twin().forId(ThingId.of(THING_NAME)).forFeature("live-data").putProperty(PLC4X_FIELD_NAME, value).get();
                logger.debug("Update in Ditto was successful");
                logger.info("Current snapshot of twin: {}", client.twin().forId(ThingId.of(THING_NAME)).retrieve().get());
                Thread.sleep(100);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void setupMock(PlcDriverManager plcDriverManager) throws PlcConnectionException {
        PlcMockConnection mockConnection = (PlcMockConnection) plcDriverManager.getConnection(PLC4X_PLC_ADDRESS);
        mockConnection.setDevice(new MockDevice() {
            @Override
            public Pair<PlcResponseCode, BaseDefaultFieldItem> read(String s) {
                return Pair.of(PlcResponseCode.OK, new DefaultIntegerFieldItem((new Random()).nextInt(100)));
            }

            @Override
            public PlcResponseCode write(String s, Object o) {
                return null;
            }

            @Override
            public Pair<PlcResponseCode, PlcSubscriptionHandle> subscribe(String s) {
                return null;
            }

            @Override
            public void unsubscribe() {

            }

            @Override
            public PlcConsumerRegistration register(Consumer<PlcSubscriptionEvent> consumer, Collection<PlcSubscriptionHandle> collection) {
                return null;
            }

            @Override
            public void unregister(PlcConsumerRegistration plcConsumerRegistration) {

            }
        });
    }
}
