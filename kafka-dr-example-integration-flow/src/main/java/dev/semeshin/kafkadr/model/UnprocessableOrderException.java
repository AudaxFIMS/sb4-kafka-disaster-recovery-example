package dev.semeshin.kafkadr.model;

/**
 * Thrown for orders no redelivery can fix. The batch handler maps it to
 * {@code discard}; retrying it would only replay the same failure forever.
 */
public class UnprocessableOrderException extends RuntimeException {
    public UnprocessableOrderException(String message) {
        super(message);
    }
}
