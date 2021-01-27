package com.dtstack.flink.sql.source.oracle.table;

import com.dtstack.flink.sql.table.AbstractSourceParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.util.MathUtil;
import org.apache.flink.connector.jdbc.table.JdbcTableSourceSinkFactory;
import org.apache.flink.table.descriptors.JdbcValidator;

import java.util.Map;

public class OracleSourceParser extends AbstractSourceParser {
    private static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    public static final String URL_KEY = "url";
    public static final String USER_NAME_KEY = "userName";
    public static final String PASSWORD_KEY = "password";
    public static final String TABLE_NAME_KEY = "tableName";



    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) throws Exception {
        OracleSourceTableInfo oracleSourceTableInfo = new OracleSourceTableInfo();
        parseFieldsInfo(fieldsInfo, oracleSourceTableInfo);
        oracleSourceTableInfo.setName(tableName);
        oracleSourceTableInfo.setType(MathUtil.getString(props.get("type")));
        JdbcTableSourceSinkFactory jdbcTableSourceSinkFactory = new JdbcTableSourceSinkFactory();
        Map<String, String> properties = jdbcTableSourceSinkFactory.requiredContext();
        properties.put(JdbcValidator.CONNECTOR_DRIVER, ORACLE_DRIVER);
        props.forEach((key, value) -> {
            if(URL_KEY.equalsIgnoreCase(key)) {
                properties.put(JdbcValidator.CONNECTOR_URL, String.valueOf(value));
            } else if(USER_NAME_KEY.equalsIgnoreCase(key)) {
                properties.put(JdbcValidator.CONNECTOR_USERNAME, String.valueOf(value));
            } else if(PASSWORD_KEY.equalsIgnoreCase(key)) {
                properties.put(JdbcValidator.CONNECTOR_PASSWORD, String.valueOf(value));
            } else if(TABLE_NAME_KEY.equalsIgnoreCase(key)) {
                properties.put(JdbcValidator.CONNECTOR_TABLE, String.valueOf(value));
            } else {
                properties.put(key, String.valueOf(value));
            }
        });
        oracleSourceTableInfo.setProps(properties);
        return oracleSourceTableInfo;
    }
}
