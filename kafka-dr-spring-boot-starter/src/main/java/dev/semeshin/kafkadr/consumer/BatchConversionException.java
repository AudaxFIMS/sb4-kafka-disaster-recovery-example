package dev.semeshin.kafkadr.consumer;

/**
 * A record inside a batch could not be converted to the handler's payload type.
 *
 * <p>Carries the position so the caller can commit the records before it and redeliver
 * from it. Failing loudly matters more in batch mode than it does per record: substituting
 * a fallback value would put a foreign type into a {@code List<T>} the handler declared,
 * and the resulting ClassCastException would surface deep inside business logic, far from
 * the record that caused it.
 */
public class BatchConversionException extends RuntimeException {

    private final int index;

    public BatchConversionException(int index, String message, Throwable cause) {
        super(message, cause);
        this.index = index;
    }

    /** Position of the offending record within the batch handed to the handler. */
    public int getIndex() {
        return index;
    }
}
