package io.github.gladwynl.streamflow.processing;

import io.github.gladwynl.streamflow.events.EventCodec;
import io.github.gladwynl.streamflow.events.EventIds;
import io.github.gladwynl.streamflow.events.EventValidator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** The plain-Java event contract components, shared by every role that reads or writes events. */
@Configuration(proxyBeanMethods = false)
class ProcessingConfiguration {

    @Bean
    EventCodec eventCodec() {
        return new EventCodec();
    }

    @Bean
    EventValidator eventValidator() {
        return new EventValidator();
    }

    @Bean
    EventIds eventIds() {
        return new EventIds();
    }
}
