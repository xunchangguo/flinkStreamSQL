package com.dtstack.flink.sql.source.clickhouse;

import org.apache.flink.connector.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.types.logical.RowType;

public class ClickhouseRowConverter extends AbstractJdbcRowConverter {

    public ClickhouseRowConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public String converterName() {
        return "Clickhouse";
    }
}
