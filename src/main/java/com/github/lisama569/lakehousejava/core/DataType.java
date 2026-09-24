package com.github.lisama569.lakehousejava.core;

/**
 * 表字段支持的数据类型。
 *
 * <p>仅实现四种基础类型，类型兼容规则明确可解释：
 * <ul>
 *     <li>{@code LONG -> DOUBLE} 是唯一允许的类型放宽（widening），因为 long 可无信息丢失地表示为 double
 *         的整数值（本实现校验时也按此规则接受）；</li>
 *     <li>其余任意类型变更（包括 DOUBLE -> LONG、STRING -> 其他等）一律拒绝。</li>
 * </ul>
 */
public enum DataType {
    STRING,
    LONG,
    DOUBLE,
    BOOLEAN;

    /**
     * 判断从当前类型变更到目标类型是否为允许的放宽变更。
     *
     * @param target 目标类型
     * @return 仅当 LONG 放宽为 DOUBLE 时返回 true；同类型也算兼容
     */
    public boolean isWideningTo(DataType target) {
        if (this == target) {
            return true;
        }
        return this == LONG && target == DOUBLE;
    }

    /**
     * 校验一个 JVM 取值是否可作为本类型的字段值。
     * 接受严格匹配；LONG 额外接受 Integer（JSON 小整数反序列化场景），
     * DOUBLE 接受 Long/Integer/Float（数值可无损转换为 double）。
     *
     * @param value 非 null 取值
     * @return true 表示类型兼容
     */
    public boolean accepts(Object value) {
        return switch (this) {
            case STRING -> value instanceof String;
            case LONG -> (value instanceof Long || value instanceof Integer);
            case DOUBLE -> (value instanceof Double || value instanceof Float
                    || value instanceof Long || value instanceof Integer);
            case BOOLEAN -> value instanceof Boolean;
        };
    }

    /**
     * 将取值规范化为规范 JVM 类型（String/Long/Double/Boolean）。
     * 调用前必须先通过 {@link #accepts(Object)}。
     */
    public Object normalize(Object value) {
        return switch (this) {
            case STRING -> (String) value;
            case LONG -> (value instanceof Long l) ? l : ((Number) value).longValue();
            case DOUBLE -> (value instanceof Double d) ? d : ((Number) value).doubleValue();
            case BOOLEAN -> (Boolean) value;
        };
    }
}
