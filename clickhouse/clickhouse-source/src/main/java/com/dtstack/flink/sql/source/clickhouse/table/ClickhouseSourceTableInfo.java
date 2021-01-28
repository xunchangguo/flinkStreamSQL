package com.dtstack.flink.sql.source.clickhouse.table;

import com.dtstack.flink.sql.table.AbstractSourceTableInfo;

import java.util.Map;

public class ClickhouseSourceTableInfo extends AbstractSourceTableInfo {
    private Map<String, String> props;

    public Map<String, String> getProps() {
        return props;
    }

    public void setProps(Map<String, String> props) {
        this.props = props;
    }
}
