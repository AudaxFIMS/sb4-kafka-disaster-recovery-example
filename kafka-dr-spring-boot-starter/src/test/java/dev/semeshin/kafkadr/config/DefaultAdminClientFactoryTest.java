package dev.semeshin.kafkadr.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

class DefaultAdminClientFactoryTest {

    @Test
    void delegatesToKafkaAdminHelper() {
        AdminClient expected = mock(AdminClient.class);
        try (MockedStatic<KafkaAdminHelper> mocked = mockStatic(KafkaAdminHelper.class)) {
            mocked.when(() -> KafkaAdminHelper.createAdminClient(anyString(), anyInt(), any()))
                    .thenReturn(expected);

            DefaultAdminClientFactory factory = new DefaultAdminClientFactory();
            AdminClient actual = factory.create("kafka:9092", 3000, Map.of("security.protocol", "SSL"));

            assertThat(actual).isSameAs(expected);
            mocked.verify(() -> KafkaAdminHelper.createAdminClient(
                    "kafka:9092", 3000, Map.of("security.protocol", "SSL")));
        }
    }
}
