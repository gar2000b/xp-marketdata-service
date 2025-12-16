package ca.digilogue.xp.model;

import java.time.LocalDateTime;

/**
 * Represents a consumer group lease entry from the consumer_group_leases table.
 * Supports lease/lock pattern for Kafka consumer groups.
 */
public class ConsumerGroupLease {

    private Long id;
    private String consumerGroupName;
    private String lockedByInstanceId;
    private LocalDateTime lockedAt;
    private LocalDateTime lockExpiresAt;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    public ConsumerGroupLease() {
    }

    public ConsumerGroupLease(Long id, String consumerGroupName, String lockedByInstanceId,
                              LocalDateTime lockedAt, LocalDateTime lockExpiresAt,
                              LocalDateTime createdAt, LocalDateTime updatedAt) {
        this.id = id;
        this.consumerGroupName = consumerGroupName;
        this.lockedByInstanceId = lockedByInstanceId;
        this.lockedAt = lockedAt;
        this.lockExpiresAt = lockExpiresAt;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getConsumerGroupName() {
        return consumerGroupName;
    }

    public void setConsumerGroupName(String consumerGroupName) {
        this.consumerGroupName = consumerGroupName;
    }

    public String getLockedByInstanceId() {
        return lockedByInstanceId;
    }

    public void setLockedByInstanceId(String lockedByInstanceId) {
        this.lockedByInstanceId = lockedByInstanceId;
    }

    public LocalDateTime getLockedAt() {
        return lockedAt;
    }

    public void setLockedAt(LocalDateTime lockedAt) {
        this.lockedAt = lockedAt;
    }

    public LocalDateTime getLockExpiresAt() {
        return lockExpiresAt;
    }

    public void setLockExpiresAt(LocalDateTime lockExpiresAt) {
        this.lockExpiresAt = lockExpiresAt;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }

    /**
     * Checks if this lease is currently available (not locked or expired).
     */
    public boolean isAvailable() {
        return lockedByInstanceId == null || 
               (lockExpiresAt != null && lockExpiresAt.isBefore(LocalDateTime.now()));
    }
}
