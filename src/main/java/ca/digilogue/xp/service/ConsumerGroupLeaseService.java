package ca.digilogue.xp.service;

import ca.digilogue.xp.App;
import ca.digilogue.xp.model.ConsumerGroupLease;
import ca.digilogue.xp.repository.ConsumerGroupLeaseRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Service for managing consumer group leases.
 * Handles lease acquisition and release for Kafka consumer groups.
 */
@Service
public class ConsumerGroupLeaseService {

    private static final Logger log = LoggerFactory.getLogger(ConsumerGroupLeaseService.class);

    private final ConsumerGroupLeaseRepository repository;

    @Value("${app.lease.duration.seconds:30}")
    private int leaseDurationSeconds;

    private ConsumerGroupLease acquiredLease;

    public ConsumerGroupLeaseService(ConsumerGroupLeaseRepository repository) {
        this.repository = repository;
    }

    /**
     * Acquires a lease for this service instance.
     * Scans available leases in order and acquires the first available one.
     * 
     * @return The acquired ConsumerGroupLease, or null if no lease is available
     */
    public ConsumerGroupLease acquireLease() {
        String instanceId = App.instanceId != null ? App.instanceId : getInstanceIdFromEnv();
        log.info("Attempting to acquire consumer group lease for instance: {} (lease duration: {} seconds)", 
                instanceId, leaseDurationSeconds);
        
        ConsumerGroupLease lease = repository.acquireLease(instanceId, leaseDurationSeconds);
        
        if (lease != null) {
            this.acquiredLease = lease;
            log.info("Successfully acquired lease: {} for instance: {}", 
                    lease.getConsumerGroupName(), instanceId);
        } else {
            log.error("Failed to acquire lease for instance: {} - no available leases", instanceId);
        }
        
        return lease;
    }

    /**
     * Releases the lease held by this service instance.
     * 
     * @return true if a lease was released, false otherwise
     */
    public boolean releaseLease() {
        String instanceId = App.instanceId != null ? App.instanceId : getInstanceIdFromEnv();
        
        if (acquiredLease == null) {
            log.warn("No lease to release for instance: {}", instanceId);
            return false;
        }
        
        boolean released = repository.releaseLease(instanceId);
        if (released) {
            this.acquiredLease = null;
            log.info("Released lease for instance: {}", instanceId);
        }
        return released;
    }
    
    /**
     * Gets instance ID from environment as fallback.
     */
    private String getInstanceIdFromEnv() {
        String envInstanceId = System.getenv("INSTANCE_ID");
        if (envInstanceId != null && !envInstanceId.isEmpty()) {
            return envInstanceId;
        }
        String hostname = System.getenv("HOSTNAME");
        if (hostname != null && !hostname.isEmpty()) {
            return hostname;
        }
        return "unknown";
    }

    /**
     * Gets the currently acquired lease.
     * 
     * @return The acquired ConsumerGroupLease, or null if no lease is acquired
     */
    public ConsumerGroupLease getAcquiredLease() {
        return acquiredLease;
    }

    /**
     * Gets the consumer group name from the acquired lease.
     * 
     * @return The consumer group name, or null if no lease is acquired
     */
    public String getAcquiredConsumerGroupName() {
        return acquiredLease != null ? acquiredLease.getConsumerGroupName() : null;
    }

    /**
     * Renews (extends) the lease expiration time.
     * This is used for keep-alive functionality to prevent lease expiration
     * while the service is running.
     * 
     * @return true if the lease was renewed, false if no lease is held
     */
    public boolean renewLease() {
        if (acquiredLease == null) {
            log.debug("No lease to renew");
            return false;
        }
        
        String instanceId = App.instanceId != null ? App.instanceId : getInstanceIdFromEnv();
        boolean renewed = repository.renewLease(instanceId, leaseDurationSeconds);
        
        if (renewed) {
            // Refresh the acquired lease to get updated expiration time
            ConsumerGroupLease updatedLease = repository.findLeaseByInstanceId(instanceId);
            if (updatedLease != null) {
                this.acquiredLease = updatedLease;
            }
        } else {
            // Lease was lost (maybe expired or released by another process)
            log.warn("Lease renewal failed - lease may have been lost for instance: {}", instanceId);
            this.acquiredLease = null;
        }
        
        return renewed;
    }
}
