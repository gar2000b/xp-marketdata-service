package ca.digilogue.xp.repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import ca.digilogue.xp.model.ConsumerGroupLease;

/**
 * Repository for managing consumer group leases.
 * Handles transactional lease acquisition for Kafka consumer groups.
 */
@Repository
public class ConsumerGroupLeaseRepository {

    private static final Logger log = LoggerFactory.getLogger(ConsumerGroupLeaseRepository.class);

    private final JdbcTemplate jdbcTemplate;

    // SQL queries
    private static final String SELECT_AVAILABLE_LEASES_SQL =
            "SELECT id, consumer_group_name, locked_by_instance_id, locked_at, lock_expires_at, created_at, updated_at " +
            "FROM consumer_group_leases " +
            "WHERE (locked_by_instance_id IS NULL OR lock_expires_at < NOW()) " +
            "ORDER BY id " +
            "LIMIT 1 " +
            "FOR UPDATE";

    private static final String ACQUIRE_LEASE_SQL =
            "UPDATE consumer_group_leases " +
            "SET locked_by_instance_id = ?, locked_at = NOW(), lock_expires_at = DATE_ADD(NOW(), INTERVAL ? SECOND) " +
            "WHERE id = ? AND (locked_by_instance_id IS NULL OR lock_expires_at < NOW())";

    private static final String RELEASE_LEASE_SQL =
            "UPDATE consumer_group_leases " +
            "SET locked_by_instance_id = NULL, locked_at = NULL, lock_expires_at = NULL " +
            "WHERE locked_by_instance_id = ?";

    private static final String FIND_BY_INSTANCE_ID_SQL =
            "SELECT id, consumer_group_name, locked_by_instance_id, locked_at, lock_expires_at, created_at, updated_at " +
            "FROM consumer_group_leases " +
            "WHERE locked_by_instance_id = ?";

    private static final String RENEW_LEASE_SQL =
            "UPDATE consumer_group_leases " +
            "SET lock_expires_at = DATE_ADD(NOW(), INTERVAL ? SECOND) " +
            "WHERE locked_by_instance_id = ? AND locked_by_instance_id IS NOT NULL";

    public ConsumerGroupLeaseRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Transactionally acquires a lease for the given instance ID.
     * Scans available leases in order (by id) and acquires the first available one.
     * 
     * @param instanceId The instance ID requesting the lease
     * @param leaseDurationSeconds How long the lease should be valid (in seconds)
     * @return The acquired ConsumerGroupLease, or null if no lease is available
     */
    @Transactional
    public ConsumerGroupLease acquireLease(String instanceId, int leaseDurationSeconds) {
        log.info("Attempting to acquire lease for instance: {} (lease duration: {} seconds)", instanceId, leaseDurationSeconds);
        
        // Find and lock the first available lease (FOR UPDATE ensures row-level locking)
        List<ConsumerGroupLease> availableLeases = jdbcTemplate.query(
                SELECT_AVAILABLE_LEASES_SQL,
                new ConsumerGroupLeaseRowMapper()
        );

        if (availableLeases.isEmpty()) {
            log.warn("No available leases found for instance: {}", instanceId);
            return null;
        }

        ConsumerGroupLease lease = availableLeases.get(0);
        log.info("Found available lease: {} (id: {}), attempting to acquire...", 
                lease.getConsumerGroupName(), lease.getId());

        // Attempt to acquire the lease atomically
        int rowsAffected = jdbcTemplate.update(
                ACQUIRE_LEASE_SQL,
                instanceId,
                leaseDurationSeconds,
                lease.getId()
        );

        if (rowsAffected == 0) {
            log.warn("Failed to acquire lease {} - another instance may have acquired it first", lease.getId());
            return null;
        }

        log.info("Successfully acquired lease: {} (id: {}) for instance: {}", 
                lease.getConsumerGroupName(), lease.getId(), instanceId);

        // Return the updated lease
        return findLeaseByInstanceId(instanceId);
    }

    /**
     * Releases a lease held by the given instance ID.
     * 
     * @param instanceId The instance ID releasing the lease
     * @return true if a lease was released, false otherwise
     */
    @Transactional
    public boolean releaseLease(String instanceId) {
        log.info("Releasing lease for instance: {}", instanceId);
        int rowsAffected = jdbcTemplate.update(RELEASE_LEASE_SQL, instanceId);
        boolean released = rowsAffected > 0;
        if (released) {
            log.info("Successfully released lease for instance: {}", instanceId);
        } else {
            log.warn("No lease found to release for instance: {}", instanceId);
        }
        return released;
    }

    /**
     * Finds the lease currently held by the given instance ID.
     * 
     * @param instanceId The instance ID
     * @return The ConsumerGroupLease, or null if not found
     */
    public ConsumerGroupLease findLeaseByInstanceId(String instanceId) {
        List<ConsumerGroupLease> leases = jdbcTemplate.query(
                FIND_BY_INSTANCE_ID_SQL,
                new ConsumerGroupLeaseRowMapper(),
                instanceId
        );
        return leases.isEmpty() ? null : leases.get(0);
    }

    /**
     * Renews (extends) the lease expiration time for the given instance ID.
     * This is used for keep-alive functionality to prevent lease expiration
     * while the service is running.
     * 
     * @param instanceId The instance ID holding the lease
     * @param leaseDurationSeconds How long to extend the lease (in seconds)
     * @return true if the lease was renewed, false if no lease was found for this instance
     */
    @Transactional
    public boolean renewLease(String instanceId, int leaseDurationSeconds) {
        int rowsAffected = jdbcTemplate.update(
                RENEW_LEASE_SQL,
                leaseDurationSeconds,
                instanceId
        );
        boolean renewed = rowsAffected > 0;
        if (renewed) {
            log.debug("Renewed lease for instance: {} (extended by {} seconds)", instanceId, leaseDurationSeconds);
        } else {
            log.warn("Failed to renew lease for instance: {} - lease may have been lost", instanceId);
        }
        return renewed;
    }

    /**
     * Row mapper for ConsumerGroupLease.
     */
    private static class ConsumerGroupLeaseRowMapper implements RowMapper<ConsumerGroupLease> {
        @Override
        public ConsumerGroupLease mapRow(ResultSet rs, int rowNum) throws SQLException {
            ConsumerGroupLease lease = new ConsumerGroupLease();
            lease.setId(rs.getLong("id"));
            lease.setConsumerGroupName(rs.getString("consumer_group_name"));
            lease.setLockedByInstanceId(rs.getString("locked_by_instance_id"));
            
            Timestamp lockedAt = rs.getTimestamp("locked_at");
            lease.setLockedAt(lockedAt != null ? lockedAt.toLocalDateTime() : null);
            
            Timestamp lockExpiresAt = rs.getTimestamp("lock_expires_at");
            lease.setLockExpiresAt(lockExpiresAt != null ? lockExpiresAt.toLocalDateTime() : null);
            
            Timestamp createdAt = rs.getTimestamp("created_at");
            lease.setCreatedAt(createdAt != null ? createdAt.toLocalDateTime() : null);
            
            Timestamp updatedAt = rs.getTimestamp("updated_at");
            lease.setUpdatedAt(updatedAt != null ? updatedAt.toLocalDateTime() : null);
            
            return lease;
        }
    }
}
