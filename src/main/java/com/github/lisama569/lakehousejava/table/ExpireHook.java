package com.github.lisama569.lakehousejava.table;

/**
 * Test-only interruption hook for snapshot expiration runs.
 *
 * <p>Used to simulate crashes at deterministic points of the GC protocol so
 * interrupt/resume safety can be asserted. The hook runs while the table lock
 * is held; throwing aborts the run at that point, leaving the on-disk state
 * exactly as it was at the hook point.</p>
 */
public interface ExpireHook {

    /** Called once per physical file deletion; throwing simulates a crash mid-deletion. */
    default void afterDeleteStep(String deletedFile) {
    }

    /** Called right after the new retention state was atomically published. */
    default void afterStatePublished(ExpireResult result) {
    }

    /** A hook that aborts (throws) after the {@code failAt}-th deleted file. */
    static ExpireHook failAfterDeletes(int failAt) {
        return new ExpireHook() {
            private int seen;

            @Override
            public void afterDeleteStep(String deletedFile) {
                seen++;
                if (seen >= failAt) {
                    throw new SimulatedInterruption("simulated interruption after " + seen + " deletion(s)");
                }
            }
        };
    }

    /** A hook that aborts right after the retention state was published (before any deletion). */
    static ExpireHook failAfterPublish() {
        return new ExpireHook() {
            @Override
            public void afterStatePublished(ExpireResult result) {
                throw new SimulatedInterruption("simulated interruption right after state publish");
            }
        };
    }

    /** Marker exception thrown by test hooks; treated as a crash, not a table error. */
    final class SimulatedInterruption extends RuntimeException {
        public SimulatedInterruption(String message) {
            super(message);
        }
    }

    /** No-op hook. */
    ExpireHook NOOP = new ExpireHook() {
    };
}
