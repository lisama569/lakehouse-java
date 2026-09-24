package com.github.lisama569.lakehousejava.exception;

/**
 * 不兼容的结构演进被拒绝（删除既有列、不允许的类型变更等）。
 */
public class SchemaEvolutionException extends TableException {
    public SchemaEvolutionException(String message) {
        super(message);
    }
}
