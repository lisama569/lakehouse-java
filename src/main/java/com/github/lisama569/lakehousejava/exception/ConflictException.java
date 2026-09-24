package com.github.lisama569.lakehousejava.exception;

/**
 * 快照隔离冲突：基于过期版本提交，或并发下版本号已被占用。
 */
public class ConflictException extends TableException {

    private final long expectedVersion;
    private final long currentVersion;

    public ConflictException(long expectedVersion, long currentVersion) {
        super("commit conflict: expected base version " + expectedVersion
                + " but current version is " + currentVersion);
        this.expectedVersion = expectedVersion;
        this.currentVersion = currentVersion;
    }

    public long expectedVersion() {
        return expectedVersion;
    }

    public long currentVersion() {
        return currentVersion;
    }
}
