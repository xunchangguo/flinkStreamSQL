package com.dtstack.flink.sql.source.sqlservercdc.table;

import com.dtstack.flink.sql.table.AbstractSourceParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;
import java.util.Properties;

public class SqlservercdcSourceParser extends AbstractSourceParser {
    private static final String HOSTNAME_KEY = "hostname";
    private static final String PORT_KEY = "port";
    private static final String USERNAME_KEY = "username";
    private static final String PASSWORD_KEY = "password";
    private static final String DATABASE_NAME_KEY = "database-name";
    private static final String TABLE_NAME_KEY = "table-name";
    private static final String SERVER_TIME_ZONE_KEY = "serverTimeZone";

    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) throws Exception {
        SqlserverCdcSourceTableInfo sqlserverCdcSourceTableInfo = new SqlserverCdcSourceTableInfo();
        parseFieldsInfo(fieldsInfo, sqlserverCdcSourceTableInfo);
        sqlserverCdcSourceTableInfo.setName(tableName);
        sqlserverCdcSourceTableInfo.setType(MathUtil.getString(props.get("type")));

        Properties properties = new Properties();
        props.forEach((key, value) -> {
            if(HOSTNAME_KEY.equalsIgnoreCase(key)) {
                sqlserverCdcSourceTableInfo.setHostname(MathUtil.getString(value));
            } else if(PORT_KEY.equalsIgnoreCase(key)) {
                sqlserverCdcSourceTableInfo.setPort(MathUtil.getIntegerVal(value, 1433));
            } else if(USERNAME_KEY.equalsIgnoreCase(key)) {
                sqlserverCdcSourceTableInfo.setUsername(MathUtil.getString(value));
            } else if(PASSWORD_KEY.equalsIgnoreCase(key)) {
                sqlserverCdcSourceTableInfo.setPassword(MathUtil.getString(value));
            } else if(DATABASE_NAME_KEY.equalsIgnoreCase(key)) {
                sqlserverCdcSourceTableInfo.setDatabaseName(MathUtil.getString(value));
            } else if(TABLE_NAME_KEY.equalsIgnoreCase(key)) {
                sqlserverCdcSourceTableInfo.setTableName(MathUtil.getString(value));
            } else if(SERVER_TIME_ZONE_KEY.equalsIgnoreCase(key)) {
                sqlserverCdcSourceTableInfo.setServerTimeZone(MathUtil.getString(value));
            } else {
                properties.put(key,value);
            }
        });
        if(!properties.isEmpty()) {
            sqlserverCdcSourceTableInfo.setProperties(properties);
        }
        return sqlserverCdcSourceTableInfo;
    }

}
