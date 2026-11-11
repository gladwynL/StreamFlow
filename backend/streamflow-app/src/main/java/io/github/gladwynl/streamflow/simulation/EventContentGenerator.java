package io.github.gladwynl.streamflow.simulation;

import io.github.gladwynl.streamflow.events.EventPayload;
import io.github.gladwynl.streamflow.events.EventPayload.ApplicationError;
import io.github.gladwynl.streamflow.events.EventPayload.CartAction;
import io.github.gladwynl.streamflow.events.EventPayload.CartUpdated;
import io.github.gladwynl.streamflow.events.EventPayload.LoginMethod;
import io.github.gladwynl.streamflow.events.EventPayload.PaymentFailed;
import io.github.gladwynl.streamflow.events.EventPayload.ProductViewed;
import io.github.gladwynl.streamflow.events.EventPayload.PurchaseCompleted;
import io.github.gladwynl.streamflow.events.EventPayload.Severity;
import io.github.gladwynl.streamflow.events.EventPayload.UserLogin;
import io.github.gladwynl.streamflow.events.EventType;
import java.util.HashMap;
import java.util.Map;
import java.util.SplittableRandom;

/**
 * Deterministic synthetic e-commerce activity.
 *
 * <p>Event {@code n} depends only on the seed, the spec's user count, {@link #CONTENT_VERSION} and {@code n}: the
 * generator is single-threaded, draws from one {@link SplittableRandom}, never reads a clock and never iterates a
 * hash map, so the sending rate, timing, send failures and thread scheduling cannot change the content. Any change
 * to the generation logic must increment {@link #CONTENT_VERSION}.
 *
 * <p>Each user has at most one session. A user without a session logs in; a user with one views products, updates
 * the cart, checks out (purchase or payment failure), hits an application error, or ends the session.
 *
 * <p>Not thread-safe.
 */
public final class EventContentGenerator {

    public static final int CONTENT_VERSION = 1;

    private static final int PRODUCTS = 2_000;
    private static final String CURRENCY = "CAD";
    private static final String[] CATEGORIES = {"books", "electronics", "home", "toys", "sports", "grocery"};
    private static final String[] PAYMENT_FAILURES = {
        "card_declined", "insufficient_funds", "expired_card", "fraud_check"
    };
    private static final String[] SERVICES = {"checkout", "catalog", "search", "auth", "cart"};
    private static final String[] ERROR_CODES = {"timeout", "upstream_5xx", "rate_limited", "bad_state"};

    private final SimulationSpec spec;
    private final SplittableRandom random;
    private final Map<Integer, Session> sessions = new HashMap<>();
    private long nextSequence;

    public EventContentGenerator(SimulationSpec spec) {
        this.spec = spec;
        this.random = new SplittableRandom(spec.seed());
    }

    public boolean hasNext() {
        return nextSequence < spec.events();
    }

    public GeneratedEvent next() {
        if (!hasNext()) {
            throw new IllegalStateException("All " + spec.events() + " events have been generated");
        }
        long sequence = nextSequence++;
        while (true) {
            int user = random.nextInt(spec.users());
            String userId = "u-%06d".formatted(user);
            Session session = sessions.get(user);
            if (session == null) {
                return login(sequence, user, userId);
            }
            GeneratedEvent event = sessionActivity(sequence, user, userId, session);
            if (event != null) {
                return event;
            }
            // The session ended without an event; draw again for the same sequence number.
        }
    }

    private GeneratedEvent login(long sequence, int user, String userId) {
        String sessionId = "s-%016x".formatted(random.nextLong());
        boolean success = random.nextInt(100) < 96;
        LoginMethod method = LoginMethod.values()[random.nextInt(LoginMethod.values().length)];
        if (success) {
            sessions.put(user, new Session(sessionId));
        }
        return event(sequence, EventType.USER_LOGIN, userId, sessionId, new UserLogin(method, success));
    }

    /** Returns {@code null} when the session ends without producing an event. */
    private GeneratedEvent sessionActivity(long sequence, int user, String userId, Session session) {
        int roll = random.nextInt(100);
        if (roll < 55) {
            return productView(sequence, userId, session);
        }
        if (roll < 75) {
            return cartUpdate(sequence, userId, session);
        }
        if (roll < 85) {
            return session.cartItems == 0
                    ? productView(sequence, userId, session)
                    : checkout(sequence, userId, session);
        }
        if (roll < 88) {
            ApplicationError error = new ApplicationError(
                    pick(SERVICES), pick(ERROR_CODES), Severity.values()[random.nextInt(Severity.values().length)]);
            return event(sequence, EventType.APPLICATION_ERROR, userId, session.id, error);
        }
        sessions.remove(user);
        return null;
    }

    private GeneratedEvent productView(long sequence, String userId, Session session) {
        int product = random.nextInt(PRODUCTS);
        ProductViewed payload = new ProductViewed(productId(product), CATEGORIES[product % CATEGORIES.length]);
        return event(sequence, EventType.PRODUCT_VIEWED, userId, session.id, payload);
    }

    private GeneratedEvent cartUpdate(long sequence, String userId, Session session) {
        int product = random.nextInt(PRODUCTS);
        boolean remove = session.cartItems > 0 && random.nextInt(4) == 0;
        int quantity = remove ? 1 : 1 + random.nextInt(3);
        if (remove) {
            session.cartValueCents -= session.cartValueCents / session.cartItems;
            session.cartItems--;
        } else {
            session.cartItems += quantity;
            session.cartValueCents += (long) quantity * priceCents(product);
        }
        CartUpdated payload =
                new CartUpdated(remove ? CartAction.REMOVE : CartAction.ADD, productId(product), quantity);
        return event(sequence, EventType.CART_UPDATED, userId, session.id, payload);
    }

    private GeneratedEvent checkout(long sequence, String userId, Session session) {
        String orderId = "o-%016x".formatted(random.nextLong());
        long amount = Math.max(1, session.cartValueCents);
        if (random.nextInt(100) < 85) {
            PurchaseCompleted payload = new PurchaseCompleted(orderId, amount, CURRENCY, session.cartItems);
            session.cartItems = 0;
            session.cartValueCents = 0;
            return event(sequence, EventType.PURCHASE_COMPLETED, userId, session.id, payload);
        }
        PaymentFailed payload = new PaymentFailed(orderId, amount, CURRENCY, pick(PAYMENT_FAILURES));
        return event(sequence, EventType.PAYMENT_FAILED, userId, session.id, payload);
    }

    private GeneratedEvent event(long sequence, EventType type, String userId, String sessionId, EventPayload payload) {
        return new GeneratedEvent(sequence, spec.scheduledOffsetMicros(sequence), type, userId, sessionId, payload);
    }

    private String pick(String[] values) {
        return values[random.nextInt(values.length)];
    }

    private static String productId(int product) {
        return "p-%04d".formatted(product);
    }

    /** A fixed price per product between $1.99 and about $301.98, independent of the random stream. */
    private static long priceCents(int product) {
        return 199 + (product * 7_919L) % 30_000;
    }

    private static final class Session {
        private final String id;
        private int cartItems;
        private long cartValueCents;

        private Session(String id) {
            this.id = id;
        }
    }
}
