package com.dtstack.flink.sql.source.postgresql;

import com.dtstack.flink.sql.source.IStreamSourceGener;
import com.dtstack.flink.sql.source.postgresql.table.PostgresqlSourceTableInfo;
import com.dtstack.flink.sql.table.AbstractSourceTableInfo;
import com.dtstack.flink.sql.util.DataTypeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.table.JdbcTableSourceSinkFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.IntStream;

public class PostgresqlSource implements IStreamSourceGener<Table> {
    @Override
    public Table genStreamSource(AbstractSourceTableInfo sourceTableInfo, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        PostgresqlSourceTableInfo postgresqlSourceTableInfo = (PostgresqlSourceTableInfo) sourceTableInfo;

        JdbcTableSourceSinkFactory jdbcTableSourceSinkFactory = new JdbcTableSourceSinkFactory();
        TableSchema tableSchema = getTableSchema(postgresqlSourceTableInfo);
        DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putTableSchema("schema", tableSchema);
        descriptorProperties.putProperties(postgresqlSourceTableInfo.getProps());

        StreamTableSource<Row> streamTableSource = jdbcTableSourceSinkFactory.createStreamTableSource(new HashMap<>(descriptorProperties.asMap()));
        String fields = StringUtils.join(postgresqlSourceTableInfo.getFields(), ",");
        return tableEnv.fromDataStream(streamTableSource.getDataStream(env), fields);
    }

    protected TableSchema getTableSchema(PostgresqlSourceTableInfo postgresqlSourceTableInfo) {
        String[] fieldTypes = postgresqlSourceTableInfo.getFieldTypes();
        Class<?>[] fieldClasses = postgresqlSourceTableInfo.getFieldClasses();
        TypeInformation[] types =
                IntStream.range(0, fieldClasses.length)
                        .mapToObj(i -> {
                            if (fieldClasses[i].isArray()) {
                                return DataTypeUtils.convertToArray(fieldTypes[i]);
                            }
                            return TypeInformation.of(fieldClasses[i]);
                        })
                        .toArray(TypeInformation[]::new);

        String[] fieldNames = postgresqlSourceTableInfo.getFields();
        DataType[] fieldDataTypes = TypeConversions.fromLegacyInfoToDataType(types);
        if (fieldNames.length != fieldDataTypes.length) {
            throw new ValidationException("Number of field names and field data types must be equal.\nNumber of names is " + fieldNames.length + ", number of data types is " + fieldTypes.length + ".\nList of field names: " + Arrays.toString(fieldNames) + "\nList of field data types: " + Arrays.toString(fieldTypes));
        }
        return TableSchema.builder().fields(fieldNames, fieldDataTypes).build();
    }
}
