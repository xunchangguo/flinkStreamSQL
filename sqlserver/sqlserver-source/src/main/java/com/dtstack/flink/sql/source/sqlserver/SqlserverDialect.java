package com.dtstack.flink.sql.source.sqlserver;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.table.types.logical.RowType;

import java.util.Optional;

public class SqlserverDialect implements JdbcDialect {
    @Override
    public String dialectName() {
        return "Sqlserver";
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("net.sourceforge.jtds.jdbc.Driver");
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:jtds:");
    }

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new SqlserverRowConverter(rowType);
    }
}
