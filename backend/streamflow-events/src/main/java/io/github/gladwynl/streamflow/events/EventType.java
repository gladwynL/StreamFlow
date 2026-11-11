package io.github.gladwynl.streamflow.events;

import io.github.gladwynl.streamflow.events.EventPayload.ApplicationError;
import io.github.gladwynl.streamflow.events.EventPayload.CartUpdated;
import io.github.gladwynl.streamflow.events.EventPayload.PaymentFailed;
import io.github.gladwynl.streamflow.events.EventPayload.ProductViewed;
import io.github.gladwynl.streamflow.events.EventPayload.PurchaseCompleted;
import io.github.gladwynl.streamflow.events.EventPayload.UserLogin;
import java.util.Optional;

/**
 * Synthetic e-commerce activity. {@link #PAYMENT_FAILED} and {@link #APPLICATION_ERROR} are business events that
 * process successfully; they are not processing failures.
 */
public enum EventType {
    USER_LOGIN(UserLogin.class),
    PRODUCT_VIEWED(ProductViewed.class),
    CART_UPDATED(CartUpdated.class),
    PURCHASE_COMPLETED(PurchaseCompleted.class),
    PAYMENT_FAILED(PaymentFailed.class),
    APPLICATION_ERROR(ApplicationError.class);

    private final Class<? extends EventPayload> payloadType;

    EventType(Class<? extends EventPayload> payloadType) {
        this.payloadType = payloadType;
    }

    public Class<? extends EventPayload> payloadType() {
        return payloadType;
    }

    /** Resolves a wire name without throwing for unknown values. */
    public static Optional<EventType> fromWireName(String name) {
        for (EventType type : values()) {
            if (type.name().equals(name)) {
                return Optional.of(type);
            }
        }
        return Optional.empty();
    }
}
