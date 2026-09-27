package com.github.lisama569.lakehousejava.table;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Retention policy configuration: defaults, validation, persistence and the rule
 * that setting a policy never deletes anything by itself.
 */
class RetentionPolicyTest {

    @TempDir
    Path tmp;

    private LocalLakehouseEngine engine;
    private TableManager t;

    @BeforeEach
    void setUp() {
        engine = new LocalLakehouseEngine(tmp);
        Schema schema = Schemas.builder()
                .column("id", DataType.LONG, false, null)
                .column("v", DataType.STRING, true, null)
                .primaryKey("id")
                .build();
        t = engine.createTable("t", schema);
    }

    private void write(long id, String v) {
        t.write(new WriteBatch(List.of(RowOp.upsert(Row.of("id", id, "v", v)))));
    }

    @Test
    @DisplayName("default policy keeps everything and setting a policy alone deletes nothing")
    void defaultKeepsEverything() {
        write(1, "a");
        write(2, "b");
        write(3, "c");

        assertNull(t.retentionPolicy().retainLatestVersions());
        assertTrue(t.retentionPolicy().pinnedVersions().isEmpty());
        assertEquals(List.of(0L, 1L, 2L, 3L), t.readableVersions());
        assertTrue(t.expiredVersions().isEmpty());

        t.setRetentionPolicy(RetentionPolicy.retainLatest(1));
        // policy alone changes nothing on disk / readability
        assertEquals(List.of(0L, 1L, 2L, 3L), t.readableVersions());
        assertTrue(t.expiredVersions().isEmpty());
        assertEquals(0, t.readSnapshot(0L).size());
        assertEquals(1, t.readSnapshot(1L).size());
        assertEquals(2, t.readSnapshot(2L).size());
        assertEquals(3, t.readSnapshot(3L).size());
    }

    @Test
    @DisplayName("retainLatest must be >= 1")
    void retainCountValidation() {
        TableException e0 = assertThrows(TableException.class,
                () -> t.setRetentionPolicy(RetentionPolicy.retainLatest(0)));
        assertEquals(RejectReasonCode.RETENTION_POLICY_INVALID, e0.code());
        TableException eNeg = assertThrows(TableException.class,
                () -> t.setRetentionPolicy(new RetentionPolicy(-3, List.of())));
        assertEquals(RejectReasonCode.RETENTION_POLICY_INVALID, eNeg.code());
    }

    @Test
    @DisplayName("pinned versions must exist (no future / negative) and pins are normalized distinct+ordered")
    void pinValidation() {
        write(1, "a"); // current = 1
        TableException future = assertThrows(TableException.class,
                () -> t.setRetentionPolicy(RetentionPolicy.pin(List.of(5L))));
        assertEquals(RejectReasonCode.RETENTION_POLICY_INVALID, future.code());
        TableException negative = assertThrows(TableException.class,
                () -> t.setRetentionPolicy(RetentionPolicy.of(1, List.of(-1L))));
        assertEquals(RejectReasonCode.RETENTION_POLICY_INVALID, negative.code());

        t.setRetentionPolicy(RetentionPolicy.of(2, List.of(1L, 1L, 0L)));
        assertEquals(List.of(0L, 1L), t.retentionPolicy().pinnedVersions(),
                "pins are de-duplicated and sorted");
    }

    @Test
    @DisplayName("cannot pin a version that was already expired")
    void cannotPinExpired() {
        write(1, "a");
        write(2, "b");
        write(3, "c"); // current = 3
        t.setRetentionPolicy(RetentionPolicy.retainLatest(1));
        t.expireSnapshots(3L);
        assertTrue(t.expiredVersions().contains(1L));

        TableException e = assertThrows(TableException.class,
                () -> t.setRetentionPolicy(RetentionPolicy.pin(List.of(1L))));
        assertEquals(RejectReasonCode.RETENTION_POLICY_INVALID, e.code());
        assertTrue(e.getMessage().contains("already expired"));
    }

    @Test
    @DisplayName("policy survives reopen; resetting to null restores keep-all intent")
    void policyPersistsAcrossReopen() {
        write(1, "a");
        t.setRetentionPolicy(RetentionPolicy.of(2, List.of(0L)));

        TableManager reopened = new LocalLakehouseEngine(tmp).openTable("t");
        assertEquals(2, reopened.retentionPolicy().retainLatestVersions());
        assertEquals(List.of(0L), reopened.retentionPolicy().pinnedVersions());

        reopened.setRetentionPolicy(null);
        assertNull(new LocalLakehouseEngine(tmp).openTable("t").retentionPolicy().retainLatestVersions());
    }

    @Test
    @DisplayName("expire with keep-all default (no cap and no pins) expires nothing")
    void expireKeepAllIsNoop() {
        write(1, "a");
        write(2, "b");
        ExpireResult r = t.expireSnapshots(null);
        assertFalse(r.committed());
        assertTrue(r.expiredVersions().isEmpty());
        assertEquals(List.of(0L, 1L, 2L), t.readableVersions());
    }
}
