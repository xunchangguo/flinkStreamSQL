package com.dtstack.flink.sql.source.oraclecdc.table;

import com.dtstack.flink.sql.table.AbstractSourceParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;
import java.util.Properties;

public class OraclecdcSourceParser extends AbstractSourceParser {
    private static final String HOSTNAME_KEY = "hostname";
    private static final String PORT_KEY = "port";
    private static final String USERNAME_KEY = "username";
    private static final String PASSWORD_KEY = "password";
    private static final String DATABASE_NAME_KEY = "database-name";
    private static final String PDB_NAME = "pdb-name";
    private static final String OUT_SERVER_NAME = "out-server-name";
    private static final String TABLE_NAME_KEY = "table-name";
    private static final String CONNECTION_ADAPTER = "connection-adapter";
    private static final String SERVER_TIME_ZONE_KEY = "serverTimeZone";

    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) throws Exception {
        OracleCdcSourceTableInfo oracleCdcSourceTableInfo = new OracleCdcSourceTableInfo();
        parseFieldsInfo(fieldsInfo, oracleCdcSourceTableInfo);
        oracleCdcSourceTableInfo.setName(tableName);
        oracleCdcSourceTableInfo.setType(MathUtil.getString(props.get("type")));

        Properties properties = new Properties();
        props.forEach((key, value) -> {
            if(HOSTNAME_KEY.equalsIgnoreCase(key)) {
                oracleCdcSourceTableInfo.setHostname(MathUtil.getString(value));
            } else if(PORT_KEY.equalsIgnoreCase(key)) {
                oracleCdcSourceTableInfo.setPort(MathUtil.getIntegerVal(value, 1433));
            } else if(USERNAME_KEY.equalsIgnoreCase(key)) {
                oracleCdcSourceTableInfo.setUsername(MathUtil.getString(value));
            } else if(PASSWORD_KEY.equalsIgnoreCase(key)) {
                oracleCdcSourceTableInfo.setPassword(MathUtil.getString(value));
            } else if(DATABASE_NAME_KEY.equalsIgnoreCase(key)) {
                oracleCdcSourceTableInfo.setDatabaseName(MathUtil.getString(value));
            } else if(TABLE_NAME_KEY.equalsIgnoreCase(key)) {
                oracleCdcSourceTableInfo.setTableName(MathUtil.getString(value));
            } else if(PDB_NAME.equalsIgnoreCase(key)) {
                oracleCdcSourceTableInfo.setPdbName(MathUtil.getString(value));
            } else if(OUT_SERVER_NAME.equalsIgnoreCase(key)) {
                oracleCdcSourceTableInfo.setOutServerName(MathUtil.getString(value));
            } else if(CONNECTION_ADAPTER.equalsIgnoreCase(key)) {
                oracleCdcSourceTableInfo.setConnectionAdapter(MathUtil.getString(value));
            } else if(SERVER_TIME_ZONE_KEY.equalsIgnoreCase(key)) {
                oracleCdcSourceTableInfo.setServerTimeZone(MathUtil.getString(value));
            } else {
                properties.put(key,value);
            }
        });
        if(!properties.isEmpty()) {
            oracleCdcSourceTableInfo.setProperties(properties);
        }
        return oracleCdcSourceTableInfo;
    }

}
