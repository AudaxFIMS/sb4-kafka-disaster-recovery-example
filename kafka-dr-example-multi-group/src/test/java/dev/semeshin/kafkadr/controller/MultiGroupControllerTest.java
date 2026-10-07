package dev.semeshin.kafkadr.controller;

import dev.semeshin.kafkadr.producer.ResilientProducer;
import dev.semeshin.kafkadr.routing.ActiveClusterManager;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MultiGroupControllerTest {

    @Test
    void auditToAnUnknownGroupIsABadRequestNotAServerError() {
        ResilientProducer producer = mock(ResilientProducer.class);
        ActiveClusterManager clusterManager = mock(ActiveClusterManager.class);
        when(clusterManager.getGroups()).thenReturn(List.of("core", "analytics"));
        MultiGroupController controller = new MultiGroupController(producer, clusterManager, null, null, null, null);

        var response = controller.sendAudit("billing", "x");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).containsEntry("groups", List.of("core", "analytics"));
        verify(producer, never()).to(anyString());
    }

    @Test
    void orderCountOutsideTheAllowedRangeIsABadRequest() {
        ResilientProducer producer = mock(ResilientProducer.class);
        MultiGroupController controller = new MultiGroupController(producer, mock(ActiveClusterManager.class),
                null, null, null, null);

        assertThat(controller.sendOrders(-1, 100, "acme").getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(controller.sendOrders(1_000_000, 100, "acme").getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        verify(producer, never()).to(anyString());
    }
}
