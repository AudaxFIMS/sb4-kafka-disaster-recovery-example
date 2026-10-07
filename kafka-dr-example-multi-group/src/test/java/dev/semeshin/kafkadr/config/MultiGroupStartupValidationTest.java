package dev.semeshin.kafkadr.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Runs the shipped {@code application.yml} through the starter's configuration checks — shared
 * brokers, names, failback-after, depends-on with its acknowledgment requirements. Handler names
 * and signatures are checked only when the application context starts, not here.
 *
 * <p>In the starter's package because the validators are package-private.
 */
class MultiGroupStartupValidationTest {

    @Test
    void shippedConfigurationPassesTheStartupChecks() throws IOException {
        KafkaClusterProperties properties = loadShippedConfiguration();

        assertThatCode(() -> {
            ClusterTopology topology = ClusterTopologyValidator.validate(properties);
            ConsumerConfigValidator.validate(properties, topology);
        }).doesNotThrowAnyException();
    }

    private static KafkaClusterProperties loadShippedConfiguration() throws IOException {
        StandardEnvironment environment = new StandardEnvironment();
        List<PropertySource<?>> sources =
                new YamlPropertySourceLoader().load("application", new ClassPathResource("application.yml"));
        sources.forEach(environment.getPropertySources()::addFirst);
        return Binder.get(environment).bind("kafka-dr", KafkaClusterProperties.class).get();
    }
}
