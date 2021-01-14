package com.dtstack.flink.sql.source.oraclecdc;

import com.alibaba.ververica.cdc.connectors.oracle.OracleSource;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.dtstack.flink.sql.source.IStreamSourceGener;
import com.dtstack.flink.sql.source.oraclecdc.table.OracleCdcSourceTableInfo;
import com.dtstack.flink.sql.source.oraclecdc.table.RowDebeziumDeserializeSchema;
import com.dtstack.flink.sql.table.AbstractSourceTableInfo;
import com.dtstack.flink.sql.util.DataTypeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.stream.IntStream;

public class OraclecdcSource implements IStreamSourceGener<Table> {

    @Override
    public Table genStreamSource(AbstractSourceTableInfo sourceTableInfo, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        OracleCdcSourceTableInfo oracleCdcSourceTableInfo = (OracleCdcSourceTableInfo) sourceTableInfo;

        TableSchema tableSchema = getTableSchema(sourceTableInfo);
        RowType rowType = (RowType) TableSchemaUtils.getPhysicalSchema(tableSchema).toRowDataType().getLogicalType();

        TypeInformation[] types = new TypeInformation[oracleCdcSourceTableInfo.getFields().length];
        for (int i = 0; i < oracleCdcSourceTableInfo.getFieldClasses().length; i++) {
            types[i] = TypeInformation.of(oracleCdcSourceTableInfo.getFieldClasses()[i]);
        }
        TypeInformation typeInformation = new RowTypeInfo(types, oracleCdcSourceTableInfo.getFields());
        RowDebeziumDeserializeSchema deserializer = new RowDebeziumDeserializeSchema(
                rowType,
                typeInformation,
                ((rowData, rowKind) -> {}),
                ZoneId.of(oracleCdcSourceTableInfo.getServerTimeZone()));

        DebeziumSourceFunction<Row> cdcSource = OracleSource.<Row>builder()
                .hostname(oracleCdcSourceTableInfo.getHostname())
                .port(oracleCdcSourceTableInfo.getPort())
                .username(oracleCdcSourceTableInfo.getUsername())
                .password(oracleCdcSourceTableInfo.getPassword())
                .database(oracleCdcSourceTableInfo.getDatabaseName())
                .tableList(oracleCdcSourceTableInfo.getTableName())
                .pdbName(oracleCdcSourceTableInfo.getPdbName())
                .outServerName(oracleCdcSourceTableInfo.getOutServerName())
                .connectionAdapter(oracleCdcSourceTableInfo.getConnectionAdapter())
                .serverTimeZone(oracleCdcSourceTableInfo.getServerTimeZone())
                .debeziumProperties(oracleCdcSourceTableInfo.getProperties())
                .deserializer(deserializer)
                .build();
        DataStreamSource dataStreamSource = env.addSource(cdcSource, sourceTableInfo.getName(), typeInformation);

        String fields = StringUtils.join(oracleCdcSourceTableInfo.getFields(), ",");
        return tableEnv.fromDataStream(dataStreamSource, fields);
    }

    protected TableSchema getTableSchema(AbstractSourceTableInfo sourceTableInfo) {
        String[] fieldTypes = sourceTableInfo.getFieldTypes();
        Class<?>[] fieldClasses = sourceTableInfo.getFieldClasses();
        TypeInformation[] types =
                IntStream.range(0, fieldClasses.length)
                        .mapToObj(i -> {
                            if (fieldClasses[i].isArray()) {
                                return DataTypeUtils.convertToArray(fieldTypes[i]);
                            }
                            return TypeInformation.of(fieldClasses[i]);
                        })
                        .toArray(TypeInformation[]::new);

        String[] fieldNames = sourceTableInfo.getFields();
        DataType[] fieldDataTypes = TypeConversions.fromLegacyInfoToDataType(types);
        if (fieldNames.length != fieldDataTypes.length) {
            throw new ValidationException("Number of field names and field data types must be equal.\nNumber of names is " + fieldNames.length + ", number of data types is " + fieldTypes.length + ".\nList of field names: " + Arrays.toString(fieldNames) + "\nList of field data types: " + Arrays.toString(fieldTypes));
        }
        return TableSchema.builder().fields(fieldNames, fieldDataTypes).build();
    }
}
