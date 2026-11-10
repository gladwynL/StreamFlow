package io.github.gladwynl.streamflow.kafka;

/** An existing topic conflicts with its specification in a way StreamFlow must not repair automatically. */
public class TopicProvisioningException extends RuntimeException {

    public TopicProvisioningException(String message) {
        super(message);
    }
}
