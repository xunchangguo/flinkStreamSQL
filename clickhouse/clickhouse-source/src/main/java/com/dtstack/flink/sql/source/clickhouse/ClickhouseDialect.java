package com.dtstack.flink.sql.source.clickhouse;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.*;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class ClickhouseDialect implements JdbcDialect {

    private static final int MAX_TIMESTAMP_PRECISION = 9;
    private static final int MIN_TIMESTAMP_PRECISION = 1;

    private static final int MAX_DECIMAL_PRECISION = 31;
    private static final int MIN_DECIMAL_PRECISION = 1;

    @Override
    public String dialectName() {
        return "Clickhouse";
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("ru.yandex.clickhouse.ClickHouseDriver");
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:clickhouse:");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new ClickhouseRowConverter(rowType);
    }

    public int maxDecimalPrecision() {
        return MAX_DECIMAL_PRECISION;
    }

    public int minDecimalPrecision() {
        return MIN_DECIMAL_PRECISION;
    }

    public int maxTimestampPrecision() {
        return MAX_TIMESTAMP_PRECISION;
    }

    public int minTimestampPrecision() {
        return MIN_TIMESTAMP_PRECISION;
    }

    public List<LogicalTypeRoot> unsupportedTypes() {
        return Arrays.asList(
                LogicalTypeRoot.BINARY,
                LogicalTypeRoot.ARRAY,
                LogicalTypeRoot.MULTISET,
                LogicalTypeRoot.MAP,
                LogicalTypeRoot.ROW,
                LogicalTypeRoot.DISTINCT_TYPE,
                LogicalTypeRoot.STRUCTURED_TYPE,
                LogicalTypeRoot.NULL,
                LogicalTypeRoot.RAW,
                LogicalTypeRoot.SYMBOL,
                LogicalTypeRoot.UNRESOLVED);
    }

    public void validate(TableSchema schema) throws ValidationException {
        for(int i = 0; i < schema.getFieldCount(); ++i) {
            DataType dt = (DataType)schema.getFieldDataType(i).get();
            String fieldName = (String)schema.getFieldName(i).get();
            if (this.unsupportedTypes().contains(dt.getLogicalType().getTypeRoot()) || dt.getLogicalType() instanceof VarBinaryType && 2147483647 != ((VarBinaryType)dt.getLogicalType()).getLength()) {
                throw new ValidationException(String.format("The %s dialect doesn't support type: %s.", this.dialectName(), dt.toString()));
            }

            int precision;
            if (dt.getLogicalType() instanceof DecimalType) {
                precision = ((DecimalType)dt.getLogicalType()).getPrecision();
                if (precision > this.maxDecimalPrecision() || precision < this.minDecimalPrecision()) {
                    throw new ValidationException(String.format("The precision of field '%s' is out of the DECIMAL precision range [%d, %d] supported by %s dialect.", fieldName, this.minDecimalPrecision(), this.maxDecimalPrecision(), this.dialectName()));
                }
            }

            if (dt.getLogicalType() instanceof TimestampType) {
                precision = ((TimestampType)dt.getLogicalType()).getPrecision();
                if (precision > this.maxTimestampPrecision() || precision < this.minTimestampPrecision()) {
                    throw new ValidationException(String.format("The precision of field '%s' is out of the TIMESTAMP precision range [%d, %d] supported by %s dialect.", fieldName, this.minTimestampPrecision(), this.maxTimestampPrecision(), this.dialectName()));
                }
            }
        }

    }
}
