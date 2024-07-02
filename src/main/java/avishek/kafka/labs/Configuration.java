package avishek.kafka.labs;

public class Configuration {
    /**
     * The bootstrap servers can have multiple values that can be comma separated.
     */
    public static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    public static final String DEFAULT_TOPIC = "quickstart-events";

    public static final String GROUP_ID = "my-app";

    public static final String VORTA_CONSUMER_GROUP = "vorta";

    public static final String MYNTRA_TOPIC = "myntra";

    public static final String AUTO_OFFSET_RESET_CONFIG_EARLIEST = "earliest";
}
