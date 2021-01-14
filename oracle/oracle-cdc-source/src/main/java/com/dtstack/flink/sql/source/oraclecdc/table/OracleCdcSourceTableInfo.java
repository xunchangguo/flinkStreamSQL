package com.dtstack.flink.sql.source.oraclecdc.table;

import com.dtstack.flink.sql.table.AbstractSourceTableInfo;

import java.util.Properties;

public class OracleCdcSourceTableInfo extends AbstractSourceTableInfo {
    private String hostname;
    private int port = 1521;
    private String username;
    private String password;
    private String databaseName;
    private String tableName;
    private String pdbName;
    private String outServerName;
    private String connectionAdapter;
    private String serverTimeZone;
    private Properties properties;

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getServerTimeZone() {
        return serverTimeZone;
    }

    public void setServerTimeZone(String serverTimeZone) {
        this.serverTimeZone = serverTimeZone;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public String getPdbName() {
        return pdbName;
    }

    public void setPdbName(String pdbName) {
        this.pdbName = pdbName;
    }

    public String getOutServerName() {
        return outServerName;
    }

    public void setOutServerName(String outServerName) {
        this.outServerName = outServerName;
    }

    public String getConnectionAdapter() {
        return connectionAdapter;
    }

    public void setConnectionAdapter(String connectionAdapter) {
        this.connectionAdapter = connectionAdapter;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
