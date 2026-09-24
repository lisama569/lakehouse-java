package com.github.lisama569.lakehousejava.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.lisama569.lakehousejava.core.ColumnDef;
import com.github.lisama569.lakehousejava.core.Row;
import com.github.lisama569.lakehousejava.core.Schema;
import com.github.lisama569.lakehousejava.exception.TableException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Stream;

/**
 * 基于本地文件系统的原子读写：所有文件先写临时文件再原子移动，避免部分生效。
 * JSON 使用 Jackson（spring-boot-starter-web 已传递引入 jsr310 时间模块）。
 */
public class LocalFileIO {

    private final ObjectMapper mapper;

    public LocalFileIO() {
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public ObjectMapper mapper() {
        return mapper;
    }

    /** 原子写 JSON：同目录临时文件 + ATOMIC_MOVE，失败不留半成品。 */
    public void writeJsonAtomically(Path target, Object value) {
        try {
            Path parent = target.toAbsolutePath().getParent();
            Files.createDirectories(parent);
            Path tmp = Files.createTempFile(parent, ".tmp-", ".json");
            try {
                mapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), value);
                try {
                    Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
                } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                    Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
                }
            } finally {
                Files.deleteIfExists(tmp);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to write " + target, e);
        }
    }

    public <T> T readJson(Path file, Class<T> type) {
        try {
            return mapper.readValue(file.toFile(), type);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read " + file, e);
        }
    }

    public void writeSnapshot(Path target, SnapshotRecord record) {
        writeJsonAtomically(target, record);
    }

    public SnapshotRecord readSnapshot(Path file) {
        return readJson(file, SnapshotRecord.class);
    }

    /**
     * 读取快照并按其结构把每个值归一化为规范 JVM 类型。
     * JSON 反序列化会把小整数读成 Integer，若不归一化，重新加载后
     * Long 主键的 equals 比较会失真，破坏可复现性。
     */
    public SnapshotRecord readSnapshotNormalized(Path file, Schema schema) {
        SnapshotRecord raw = readJson(file, SnapshotRecord.class);
        java.util.List<Row> rows = new java.util.ArrayList<>();
        for (Row row : raw.rows()) {
            Row fixed = new Row();
            for (ColumnDef def : schema.columns()) {
                Object v = row.get(def.name());
                if (v != null && def.type().accepts(v)) {
                    v = def.type().normalize(v);
                }
                fixed.put(def.name(), v);
            }
            rows.add(fixed);
        }
        return new SnapshotRecord(raw.version(), raw.parentVersion(), raw.schemaId(),
                raw.operation(), raw.createdAt(), raw.description(), rows);
    }

    /** 列出目录下指定后缀的文件，按文件名排序（版本号零填充 => 字典序即版本序）。 */
    public List<Path> listFiles(Path dir, String suffix) {
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        try (Stream<Path> s = Files.list(dir)) {
            return s.filter(p -> p.getFileName().toString().endsWith(suffix))
                    .sorted()
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to list " + dir, e);
        }
    }

    public String readString(Path file) {
        try {
            return Files.readString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 计算文件原始字节的 SHA-256（整理前后对比用，保证结论不漂移）。 */
    public String sha256(Path file) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            try (var in = Files.newInputStream(file)) {
                in.transferTo(new java.io.OutputStream() {
                    @Override
                    public void write(int b) {
                        md.update((byte) b);
                    }

                    @Override
                    public void write(byte[] b, int off, int len) {
                        md.update(b, off, len);
                    }
                });
            }
            return HexFormat.of().formatHex(md.digest());
        } catch (Exception e) {
            throw new TableException("sha256 failed for " + file + ": " + e.getMessage());
        }
    }
}
