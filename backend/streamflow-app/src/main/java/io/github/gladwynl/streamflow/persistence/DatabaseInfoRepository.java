package io.github.gladwynl.streamflow.persistence;

import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Read-only facts about the connected PostgreSQL database. */
@Repository
public class DatabaseInfoRepository {

    private final JdbcClient jdbc;

    public DatabaseInfoRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public String serverVersion() {
        return jdbc.sql("SHOW server_version").query(String.class).single();
    }

    /** The version of the latest successfully applied Flyway migration, if any. */
    public Optional<String> schemaVersion() {
        return jdbc.sql("""
                        SELECT version FROM flyway_schema_history
                        WHERE success AND version IS NOT NULL
                        ORDER BY installed_rank DESC
                        LIMIT 1
                        """).query(String.class).optional();
    }
}
