package ca.digilogue.xp.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * Service for keeping the consumer group lease alive.
 * Periodically renews the lease expiration time to prevent it from expiring
 * while the service is running. If the service crashes, the lease will expire
 * naturally and another instance can acquire it.
 */
@Service
public class LeaseKeepAliveService {

    private static final Logger log = LoggerFactory.getLogger(LeaseKeepAliveService.class);

    private final ConsumerGroupLeaseService leaseService;

    @Value("${app.lease.renewal.interval.seconds:20}")
    private int renewalIntervalSeconds;

    public LeaseKeepAliveService(ConsumerGroupLeaseService leaseService) {
        this.leaseService = leaseService;
    }

    /**
     * Periodically renews the lease to keep it alive.
     * Runs every N seconds (configured via app.lease.renewal.interval.seconds).
     * The renewal interval should be less than the lease duration to ensure
     * the lease never expires while the service is healthy.
     * 
     * Note: fixedDelayString uses SpEL to convert seconds to milliseconds.
     * The property must be set (no default in SpEL to avoid parsing issues).
     */
    @Scheduled(fixedDelayString = "#{${app.lease.renewal.interval.seconds} * 1000}")
    public void keepLeaseAlive() {
        try {
            boolean renewed = leaseService.renewLease();
            if (!renewed) {
                log.warn("Lease keep-alive failed - no lease is currently held. " +
                        "This may indicate the lease was lost or never acquired.");
            }
        } catch (Exception e) {
            log.error("Error during lease keep-alive renewal", e);
            // Don't throw - we want the scheduled task to continue running
            // even if one renewal attempt fails
        }
    }
}
