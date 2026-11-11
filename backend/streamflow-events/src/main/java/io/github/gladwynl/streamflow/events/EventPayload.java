package io.github.gladwynl.streamflow.events;

/**
 * Type-specific event content. Fields are boxed so a missing field decodes to {@code null} and is reported by
 * {@link EventValidator} instead of silently becoming a default value. Money is always integer cents.
 */
public sealed interface EventPayload {

    enum LoginMethod {
        PASSWORD,
        SSO,
        MAGIC_LINK
    }

    enum CartAction {
        ADD,
        REMOVE
    }

    enum Severity {
        WARNING,
        ERROR,
        CRITICAL
    }

    record UserLogin(LoginMethod method, Boolean success) implements EventPayload {}

    record ProductViewed(String productId, String category) implements EventPayload {}

    record CartUpdated(CartAction action, String productId, Integer quantity) implements EventPayload {}

    record PurchaseCompleted(String orderId, Long amountCents, String currency, Integer itemCount)
            implements EventPayload {}

    record PaymentFailed(String orderId, Long amountCents, String currency, String reasonCode)
            implements EventPayload {}

    record ApplicationError(String service, String errorCode, Severity severity) implements EventPayload {}
}
