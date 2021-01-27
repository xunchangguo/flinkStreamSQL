package com.dtstack.flink.sql.source.oracle;

import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.SQLException;

public class OracleInputFormat extends JdbcInputFormat {
    @Override
    public Row nextRecord(Row reuse) throws IOException {
        try {
            if (!this.hasNext) {
                return null;
            } else {
                for(int pos = 0; pos < reuse.getArity(); ++pos) {
                    reuse.setField(pos, this.resultSet.getObject(pos + 1));
                }

                this.hasNext = this.resultSet.next();
                return reuse;
            }
        } catch (SQLException var3) {
            throw new IOException("Couldn't read data - " + var3.getMessage(), var3);
        } catch (NullPointerException var4) {
            throw new IOException("Couldn't access resultSet", var4);
        }
    }
}
