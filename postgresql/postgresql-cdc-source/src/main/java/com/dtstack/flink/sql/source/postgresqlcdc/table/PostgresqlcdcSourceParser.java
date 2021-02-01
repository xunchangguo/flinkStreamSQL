package com.dtstack.flink.sql.source.postgresqlcdc.table;

import com.dtstack.flink.sql.table.AbstractSourceParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;
import java.util.Properties;

public class PostgresqlcdcSourceParser extends AbstractSourceParser {
    private static final String PLUGIN_NAME_KEY = "pluginName";
    private static final String HOSTNAME_KEY = "hostname";
    private static final String PORT_KEY = "port";
    private static final String USERNAME_KEY = "username";
    private static final String PASSWORD_KEY = "password";
    private static final String DATABASE_NAME_KEY = "database-name";
    private static final String TABLE_NAME_KEY = "table-name";
    private static final String SERVER_TIME_ZONE_KEY = "serverTimeZone";
    private static final String SCHEMA_NAME_KEY = "schema";

    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) throws Exception {
        PostgresqlCdcSourceTableInfo postgresqlCdcSourceTableInfo = new PostgresqlCdcSourceTableInfo();
        parseFieldsInfo(fieldsInfo, postgresqlCdcSourceTableInfo);
        postgresqlCdcSourceTableInfo.setName(tableName);
        postgresqlCdcSourceTableInfo.setType(MathUtil.getString(props.get("type")));

        Properties properties = new Properties();
        props.forEach((key, value) -> {
            if(HOSTNAME_KEY.equalsIgnoreCase(key)) {
                postgresqlCdcSourceTableInfo.setHostname(MathUtil.getString(value));
            } else if(PORT_KEY.equalsIgnoreCase(key)) {
                postgresqlCdcSourceTableInfo.setPort(MathUtil.getIntegerVal(value, 5432));
            } else if(USERNAME_KEY.equalsIgnoreCase(key)) {
                postgresqlCdcSourceTableInfo.setUsername(MathUtil.getString(value));
            } else if(PASSWORD_KEY.equalsIgnoreCase(key)) {
                postgresqlCdcSourceTableInfo.setPassword(MathUtil.getString(value));
            } else if(DATABASE_NAME_KEY.equalsIgnoreCase(key)) {
                postgresqlCdcSourceTableInfo.setDatabaseName(MathUtil.getString(value));
            } else if(TABLE_NAME_KEY.equalsIgnoreCase(key)) {
                postgresqlCdcSourceTableInfo.setTableName(MathUtil.getString(value));
            } else if(SCHEMA_NAME_KEY.equalsIgnoreCase(key)) {
                postgresqlCdcSourceTableInfo.setSchema(MathUtil.getString(value));
            } else if(SERVER_TIME_ZONE_KEY.equalsIgnoreCase(key)) {
                postgresqlCdcSourceTableInfo.setServerTimeZone(MathUtil.getString(value));
            } else if(PLUGIN_NAME_KEY.equalsIgnoreCase(key)) {
                postgresqlCdcSourceTableInfo.setPluginName(MathUtil.getString(value));
            } else {
                properties.put(key,value);
            }
        });
        if(!properties.isEmpty()) {
            postgresqlCdcSourceTableInfo.setProperties(properties);
        }
        return postgresqlCdcSourceTableInfo;
    }

}
