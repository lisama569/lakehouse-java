package com.github.lisama569.lakehousejava.tests;

import com.github.lisama569.lakehousejava.core.ColumnDef;
import com.github.lisama569.lakehousejava.core.CommitResult;
import com.github.lisama569.lakehousejava.core.DataType;
import com.github.lisama569.lakehousejava.core.Row;
import com.github.lisama569.lakehousejava.core.RowOp;
import com.github.lisama569.lakehousejava.core.WriteRequest;
import com.github.lisama569.lakehousejava.exception.ConflictException;
import com.github.lisama569.lakehousejava.table.LocalLakehouse;
import com.github.lisama569.lakehousejava.table.LocalTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景四：并发提交满足快照隔离。
 * 所有线程都基于 v0 提交时，恰好一个成功，其余全部以 {@link ConflictException} 被拒绝；
 * 失败方重试（基于最新版本）后全部成功，最终行数唯一确定、无丢失无重复。
 */
class ConcurrentCommitTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentCommitTest.class);

    private LocalTable table(Path dir) {
        return new LocalLakehouse(dir).createTableIfAbsent("c", "id", List.of(
                new ColumnDef("id", DataType.LONG, null),
                new ColumnDef("v", DataType.STRING, null)));
    }

    private WriteRequest req(long base, long id, String v) {
        return new WriteRequest(base, List.of(
                new WriteRequest.RowOperation(RowOp.UPSERT, Row.of("id", id, "v", v))));
    }

    @Test
    void staleBaseVersionIsRejected(@TempDir Path dir) {
        LocalTable t = table(dir);
        t.write(req(0L, 1L, "a"));
        ConflictException e = assertThrows(ConflictException.class,
                () -> t.write(req(0L, 2L, "b")));
        log.info("过期版本提交被拒绝: 期望 v{}, 当前 v{}", e.expectedVersion(), e.currentVersion());
        assertEquals(0L, e.expectedVersion());
        assertEquals(1L, e.currentVersion());
        // 被拒绝的写入未部分生效
        assertEquals(1, t.readLatest().size());
    }

    @Test
    void concurrentSameBaseCommitsExactlyOneWins(@TempDir Path dir) throws Exception {
        LocalTable t = table(dir);
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger ok = new AtomicInteger();
        ConcurrentLinkedQueue<Long> rejectedBases = new ConcurrentLinkedQueue<>();

        for (int i = 1; i <= threads; i++) {
            final long id = i;
            pool.submit(() -> {
                try {
                    start.await();
                    // 所有线程都基于 v0 提交
                    t.write(req(0L, id, "w" + id));
                    ok.incrementAndGet();
                } catch (ConflictException c) {
                    rejectedBases.add(c.currentVersion());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        log.info("并发基于 v0 的提交: 线程数={}, 成功={}, 被拒={}, 被拒时当前版本集合={}",
                threads, ok.get(), rejectedBases.size(), rejectedBases);
        assertEquals(1, ok.get(), "快照隔离下仅允许一个提交成功");
        assertEquals(threads - 1, rejectedBases.size());
        assertTrue(rejectedBases.stream().allMatch(v -> v == 1L));
        assertEquals(1, t.currentVersion());
        assertEquals(1, t.readLatest().size());
    }

    @Test
    void retryOnConflictConvergesToUniqueFinalState(@TempDir Path dir) throws Exception {
        LocalTable t = table(dir);
        int n = 10;
        ExecutorService pool = Executors.newFixedThreadPool(4);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger committed = new AtomicInteger();

        for (int i = 1; i <= n; i++) {
            final long id = i;
            pool.submit(() -> {
                try {
                    start.await();
                    while (true) {
                        attempts.incrementAndGet();
                        long base = t.currentVersion();
                        try {
                            CommitResult r = t.write(req(base, id, "v" + id));
                            committed.incrementAndGet();
                            log.info("线程 id={} 基于 v{} 提交成功 -> v{}", id, base, r.version());
                            return;
                        } catch (ConflictException c) {
                            log.info("线程 id={} 基于过期 v{} 冲突(当前 v{})，重试",
                                    id, base, c.currentVersion());
                        }
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(15, TimeUnit.SECONDS));

        assertEquals(n, committed.get());
        assertEquals(n, t.currentVersion(), "每次成功提交产生一个新版本");
        List<Row> rows = t.readLatest();
        assertEquals(n, rows.size(), "无丢失、无重复计数");
        for (long id = 1; id <= n; id++) {
            final long key = id;
            assertEquals("v" + key, rows.stream()
                    .filter(r -> r.get("id").equals(key)).findFirst().orElseThrow().get("v"));
        }
        log.info("重试收敛完成: 尝试次数={}, 成功提交={}, 最终版本=v{}, 最终行数={}（唯一确定）",
                attempts.get(), committed.get(), t.currentVersion(), rows.size());
    }
}
