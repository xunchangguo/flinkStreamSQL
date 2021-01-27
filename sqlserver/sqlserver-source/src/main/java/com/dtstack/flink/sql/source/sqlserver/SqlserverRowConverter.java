package com.dtstack.flink.sql.source.sqlserver;

import org.apache.flink.connector.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.types.logical.RowType;

public class SqlserverRowConverter extends AbstractJdbcRowConverter {

    public SqlserverRowConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public String converterName() {
        return "Sqlserver";
    }
}
