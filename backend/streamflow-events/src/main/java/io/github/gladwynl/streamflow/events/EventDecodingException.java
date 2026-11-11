package io.github.gladwynl.streamflow.events;

/** The bytes are not a well-formed event of a supported schema version. */
public class EventDecodingException extends RuntimeException {

    public EventDecodingException(String message) {
        super(message);
    }

    public EventDecodingException(String message, Throwable cause) {
        super(message, cause);
    }
}
