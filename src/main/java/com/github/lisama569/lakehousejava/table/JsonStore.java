package com.github.lisama569.lakehousejava.table;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

/**
 * JSON file persistence with atomic replacement (temp file + atomic move).
 */
final class JsonStore {

    private final ObjectMapper mapper;

    JsonStore() {
        this(new ObjectMapper());
    }

    JsonStore(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    <T> T read(Path file, Class<T> type) {
        try {
            return mapper.readValue(Files.readAllBytes(file), type);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read " + file, e);
        }
    }

    <T> List<T> readList(Path file, Class<T> elementType) {
        try {
            return mapper.readValue(Files.readAllBytes(file),
                    mapper.getTypeFactory().constructCollectionType(List.class, elementType));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read list " + file, e);
        }
    }

    /** Write atomically: serialize to a sibling temp file, then move it into place. */
    void writeAtomic(Path file, Object value) {
        Path parent = file.toAbsolutePath().getParent();
        try {
            Files.createDirectories(parent);
            Path tmp = Files.createTempFile(parent, file.getFileName().toString(), ".tmp");
            try {
                mapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), value);
                try {
                    Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE);
                } catch (IOException atomicFailed) {
                    Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
                }
            } finally {
                Files.deleteIfExists(tmp);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to write " + file, e);
        }
    }
}
