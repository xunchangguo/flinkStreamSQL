package com.dtstack.flink.sql.source.postgresql.table;

import com.dtstack.flink.sql.table.AbstractSourceTableInfo;

import java.util.Map;

public class PostgresqlSourceTableInfo extends AbstractSourceTableInfo {
    private Map<String, String> props;

    public Map<String, String> getProps() {
        return props;
    }

    public void setProps(Map<String, String> props) {
        this.props = props;
    }
}
