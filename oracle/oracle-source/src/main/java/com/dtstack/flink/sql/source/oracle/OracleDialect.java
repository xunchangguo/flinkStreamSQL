package com.dtstack.flink.sql.source.oracle;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.table.types.logical.RowType;

import java.util.Optional;

public class OracleDialect implements JdbcDialect {
    @Override
    public String dialectName() {
        return "Oracle";
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("oracle.jdbc.driver.OracleDriver");
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:oracle:");
    }

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new OracleRowConverter(rowType);
    }
}
