package com.github.lisama569.lakehousejava.exception;

/**
 * 表操作相关受检语义的非受检异常基类。
 */
public class TableException extends RuntimeException {
    public TableException(String message) {
        super(message);
    }
}
