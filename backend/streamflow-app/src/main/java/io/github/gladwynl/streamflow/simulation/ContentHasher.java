package io.github.gladwynl.streamflow.simulation;

import io.github.gladwynl.streamflow.events.EventCodec;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** SHA-256 over the canonical JSON of a sequence of {@link GeneratedEvent}s, used to prove determinism. */
public final class ContentHasher {

    private final EventCodec codec;
    private final MessageDigest digest;

    public ContentHasher(EventCodec codec) {
        this.codec = codec;
        try {
            this.digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by every Java platform", e);
        }
    }

    public void add(GeneratedEvent event) {
        digest.update(codec.canonicalJson(event));
        digest.update((byte) '\n');
    }

    public String hex() {
        return HexFormat.of().formatHex(digest.digest());
    }

    /** Hash of the first {@code count} events of a spec. */
    public static String hashOf(SimulationSpec spec, long count, EventCodec codec) {
        EventContentGenerator generator = new EventContentGenerator(spec);
        ContentHasher hasher = new ContentHasher(codec);
        for (long i = 0; i < count && generator.hasNext(); i++) {
            hasher.add(generator.next());
        }
        return hasher.hex();
    }
}
