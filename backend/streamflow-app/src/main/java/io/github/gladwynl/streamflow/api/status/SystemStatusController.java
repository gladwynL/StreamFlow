package io.github.gladwynl.streamflow.api.status;

import org.springframework.context.annotation.Profile;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Read-only system status. Always answers 200; a degraded dependency is reported in the body, not as an error. */
@RestController
@RequestMapping("/api/v1/system")
@Profile("api")
class SystemStatusController {

    private final SystemStatusService service;

    SystemStatusController(SystemStatusService service) {
        this.service = service;
    }

    @GetMapping("/status")
    ResponseEntity<SystemStatus> status() {
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(service.currentStatus());
    }
}
