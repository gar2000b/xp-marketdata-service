package ca.digilogue.xp.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import ca.digilogue.xp.App;
import ca.digilogue.xp.model.ConsumerGroupLease;
import ca.digilogue.xp.service.ConsumerGroupLeaseService;

/**
 * Configuration that acquires the consumer group lease on application startup.
 * This ensures the lease is acquired before Kafka beans are created.
 */
@Configuration
public class LeaseAcquisitionConfig {

    private static final Logger log = LoggerFactory.getLogger(LeaseAcquisitionConfig.class);

    private final ConsumerGroupLeaseService leaseService;

    public LeaseAcquisitionConfig(ConsumerGroupLeaseService leaseService) {
        this.leaseService = leaseService;
    }

    /**
     * Bean that triggers lease acquisition.
     * Other beans can depend on this to ensure lease is acquired first.
     */
    @org.springframework.context.annotation.Bean
    public String leaseAcquisition() {
        log.info("Acquiring consumer group lease for instance: {}", App.instanceId);
        
        ConsumerGroupLease lease = leaseService.acquireLease();
        
        if (lease == null) {
            log.error("CRITICAL: Failed to acquire consumer group lease for instance: {}. Service cannot start.", App.instanceId);
            throw new IllegalStateException("Failed to acquire consumer group lease - no available leases");
        }
        
        App.acquiredConsumerGroupName = lease.getConsumerGroupName();
        log.info("========================================");
        log.info("Successfully acquired consumer group lease:");
        log.info("  Consumer Group: '{}'", App.acquiredConsumerGroupName);
        log.info("  Instance ID: '{}'", App.instanceId);
        log.info("  Lease ID: {}", lease.getId());
        log.info("========================================");
        
        return App.acquiredConsumerGroupName;
    }
}
