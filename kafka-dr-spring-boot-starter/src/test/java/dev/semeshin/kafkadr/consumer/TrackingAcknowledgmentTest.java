package dev.semeshin.kafkadr.consumer;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TrackingAcknowledgmentTest {

    private final Acknowledgment delegate = mock(Acknowledgment.class);
    private final TrackingAcknowledgment tracking = new TrackingAcknowledgment(delegate);

    @Test
    void everyCallReachesTheContainer() {
        tracking.acknowledge();
        tracking.acknowledge(3);
        tracking.nack(Duration.ofSeconds(2));
        tracking.nack(1, Duration.ofSeconds(2));

        verify(delegate).acknowledge();
        verify(delegate).acknowledge(3);
        verify(delegate).nack(Duration.ofSeconds(2));
        verify(delegate).nack(1, Duration.ofSeconds(2));
    }

    @Test
    void indexedAcknowledgmentCountsAsACommit() {
        assertThat(tracking.isAcknowledged()).isFalse();

        tracking.acknowledge(2);

        assertThat(tracking.isAcknowledged()).isTrue();
        assertThat(tracking.isNacked()).isFalse();
    }

    @Test
    void nackIsRememberedSeparatelyFromACommit() {
        tracking.nack(1, Duration.ofSeconds(1));

        // A redelivery request is not a forgotten acknowledgment, and it is not a commit either.
        assertThat(tracking.isNacked()).isTrue();
        assertThat(tracking.isAcknowledged()).isFalse();
    }

    @Test
    void aRejectedAcknowledgmentIsNotACommit() {
        doThrow(new UnsupportedOperationException("not a list listener")).when(delegate).acknowledge(1);

        assertThatThrownBy(() -> tracking.acknowledge(1)).isInstanceOf(UnsupportedOperationException.class);

        // The watermark must not follow a call the container refused.
        assertThat(tracking.isAcknowledged()).isFalse();
    }

    @Test
    void outOfOrderCommitReflectsTheContainer() {
        when(delegate.isOutOfOrderCommit()).thenReturn(true);

        assertThat(tracking.isOutOfOrderCommit()).isTrue();
    }
}
