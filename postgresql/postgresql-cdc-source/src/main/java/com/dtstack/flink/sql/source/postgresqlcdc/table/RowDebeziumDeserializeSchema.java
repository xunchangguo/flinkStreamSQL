package com.dtstack.flink.sql.source.postgresqlcdc.table;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.utils.TemporalConversions;
import io.debezium.data.Envelope;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.time.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class RowDebeziumDeserializeSchema implements DebeziumDeserializationSchema<Row> {
    private static final long serialVersionUID = -4852684966051743776L;
    private static final String FIELD_OP = "op";
    private static final String FIELD_SOURCE_DB = "source_db";
    private static final String FIELD_STRUCT_DB = "db";
    private static final String FIELD_SOURCE_SCHEMA = "source_schema";
    private static final String FIELD_STRUCT_SCHEMA = "schema";
    private static final String FIELD_SOURCE_TABLE = "source_table";
    private static final String FIELD_STRUCT_TABLE = "table";

    /**
     * Custom validator to validate the row value.
     */
    public interface ValueValidator extends Serializable {
        void validate(Row row, RowKind rowKind) throws Exception;
    }

    /** TypeInformation of the produced {@link Row}. **/
    private final TypeInformation<Row> resultTypeInfo;

    /**
     * Runtime converter that converts {@link JsonNode}s into
     * objects of Flink SQL internal data structures. **/
    private final RowDebeziumDeserializeSchema.DeserializationRuntimeConverter runtimeConverter;

    /**
     * Time zone of the database server.
     */
    private final ZoneId serverTimeZone;

    /**
     * Validator to validate the row value.
     */
    private final RowDebeziumDeserializeSchema.ValueValidator validator;

    public RowDebeziumDeserializeSchema(RowType rowType, TypeInformation<Row> resultTypeInfo, RowDebeziumDeserializeSchema.ValueValidator validator, ZoneId serverTimeZone) {
        this.runtimeConverter = createConverter(rowType);
        this.resultTypeInfo = resultTypeInfo;
        this.validator = validator;
        this.serverTimeZone = serverTimeZone;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<Row> out) throws Exception {
        Envelope.Operation op = Envelope.operationFor(record);
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
        if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
            Row insert = extractAfterRow(value, valueSchema, op);
            validator.validate(insert, RowKind.INSERT);
//            insert.setRowKind(RowKind.INSERT);
            out.collect(insert);
        } else if (op == Envelope.Operation.DELETE) {
            //TODO
            Row delete = extractBeforeRow(value, valueSchema, op);
            validator.validate(delete, RowKind.DELETE);
//            delete.setRowKind(RowKind.DELETE);
            out.collect(delete);
        } else {
            //TODO
            //update
//            Row before = extractBeforeRow(value, valueSchema);
//            validator.validate(before, RowKind.UPDATE_BEFORE);
//            before.setRowKind(RowKind.UPDATE_BEFORE);
//            out.collect(before);

            Row after = extractAfterRow(value, valueSchema, op);
            validator.validate(after, RowKind.UPDATE_AFTER);
//            after.setRowKind(RowKind.UPDATE_AFTER);
            out.collect(after);
        }
    }

    private Row extractAfterRow(Struct value, Schema valueSchema, Envelope.Operation op) throws Exception {
        Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        return (Row) runtimeConverter.convert(after, afterSchema, op, source);
    }

    private Row extractBeforeRow(Struct value, Schema valueSchema, Envelope.Operation op) throws Exception {
        Schema afterSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
        Struct after = value.getStruct(Envelope.FieldName.BEFORE);
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        return (Row) runtimeConverter.convert(after, afterSchema, op, source);
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return resultTypeInfo;
    }

    // -------------------------------------------------------------------------------------
    // Runtime Converters
    // -------------------------------------------------------------------------------------

    /**
     * Runtime converter that converts objects of Debezium into objects of Flink Table & SQL internal data structures.
     */
    @FunctionalInterface
    private interface DeserializationRuntimeConverter extends Serializable {
        Object convert(Object dbzObj, Schema schema, Envelope.Operation op, Struct source) throws Exception;
    }

    /**
     * Creates a runtime converter which is null safe.
     */
    private RowDebeziumDeserializeSchema.DeserializationRuntimeConverter createConverter(LogicalType type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    /**
     * Creates a runtime converter which assuming input object is not null.
     */
    private RowDebeziumDeserializeSchema.DeserializationRuntimeConverter createNotNullConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return (dbzObj, schema, op, source) -> null;
            case BOOLEAN:
                return (dbzObj1, schema1, op1, source) -> convertToBoolean(dbzObj1, schema1, op1);
            case TINYINT:
                return (dbzObj, schema, op, source) -> Byte.parseByte(dbzObj.toString());
            case SMALLINT:
                return (dbzObj, schema, op, source) -> Short.parseShort(dbzObj.toString());
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return (dbzObj1, schema1, op1, source) -> convertToInt(dbzObj1, schema1, op1);
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (dbzObj1, schema1, op1, source) -> convertToLong(dbzObj1, schema1, op1);
            case DATE:
                return (dbzObj, schema, op, source) -> convertToDate(dbzObj, schema, op);
            case TIME_WITHOUT_TIME_ZONE:
                return (dbzObj, schema, op, source) -> convertToTime(dbzObj, schema, op);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (dbzObj, schema, op, source) -> convertToTimestamp(dbzObj, schema, op);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (dbzObj, schema, op, source) -> convertToLocalTimeZoneTimestamp(dbzObj, schema, op);
            case FLOAT:
                return (dbzObj, schema, op, source) -> convertToFloat(dbzObj, schema, op);
            case DOUBLE:
                return (dbzObj1, schemam, op1, source) -> convertToDouble(dbzObj1, schemam, op1);
            case CHAR:
            case VARCHAR:
                return (dbzObj1, schema1, op1, source) -> convertToString(dbzObj1, schema1, op1);
            case BINARY:
            case VARBINARY:
                return (dbzObj, schema, op, source) -> convertToBinary(dbzObj, schema, op);
            case DECIMAL:
                return createDecimalConverter((DecimalType) type);
            case ROW:
                return createRowConverter((RowType) type);
            case ARRAY:
            case MAP:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private boolean convertToBoolean(Object dbzObj, Schema schema, Envelope.Operation op) {
        if (dbzObj instanceof Boolean) {
            return (boolean) dbzObj;
        } else if (dbzObj instanceof Byte) {
            return (byte) dbzObj == 1;
        } else if (dbzObj instanceof Short) {
            return (short) dbzObj == 1;
        } else {
            return Boolean.parseBoolean(dbzObj.toString());
        }
    }

    private int convertToInt(Object dbzObj, Schema schema, Envelope.Operation op) {
        if (dbzObj instanceof Integer) {
            return (int) dbzObj;
        } else if (dbzObj instanceof Long) {
            return ((Long) dbzObj).intValue();
        } else {
            return Integer.parseInt(dbzObj.toString());
        }
    }

    private long convertToLong(Object dbzObj, Schema schema, Envelope.Operation op) {
        if (dbzObj instanceof Integer) {
            return ((Integer) dbzObj).longValue();
        } else if (dbzObj instanceof Long) {
            return (long) dbzObj;
        } else {
            return Long.parseLong(dbzObj.toString());
        }
    }

    private double convertToDouble(Object dbzObj, Schema schemam, Envelope.Operation op) {
        if (dbzObj instanceof Float) {
            return (double) dbzObj;
        } else if (dbzObj instanceof Double) {
            return (double) dbzObj;
        } else {
            return Double.parseDouble(dbzObj.toString());
        }
    }

    private float convertToFloat(Object dbzObj, Schema schema, Envelope.Operation op) {
        if (dbzObj instanceof Float) {
            return (float) dbzObj;
        } else if (dbzObj instanceof Double) {
            return ((Double) dbzObj).floatValue();
        } else {
            return Float.parseFloat(dbzObj.toString());
        }
    }

    private int convertToDate(Object dbzObj, Schema schema, Envelope.Operation op) {
        return (int) TemporalConversions.toLocalDate(dbzObj).toEpochDay();
    }

    private int convertToTime(Object dbzObj, Schema schema, Envelope.Operation op) {
        if (dbzObj instanceof Long) {
            switch (schema.name()) {
                case MicroTime.SCHEMA_NAME:
                    return (int) ((long) dbzObj / 1000);
                case NanoTime.SCHEMA_NAME:
                    return (int) ((long) dbzObj / 1000_000);
            }
        } else if (dbzObj instanceof Integer) {
            return (int) dbzObj;
        }
        // get number of milliseconds of the day
        return TemporalConversions.toLocalTime(dbzObj).toSecondOfDay() * 1000;
    }

    private TimestampData convertToTimestamp(Object dbzObj, Schema schema, Envelope.Operation op) {
        if (dbzObj instanceof Long) {
            switch (schema.name()) {
                case Timestamp.SCHEMA_NAME:
                    return TimestampData.fromEpochMillis((Long) dbzObj);
                case MicroTimestamp.SCHEMA_NAME:
                    long micro = (long) dbzObj;
                    return TimestampData.fromEpochMillis(micro / 1000, (int) (micro % 1000 * 1000));
                case NanoTimestamp.SCHEMA_NAME:
                    long nano = (long) dbzObj;
                    return TimestampData.fromEpochMillis(nano / 1000_000, (int) (nano % 1000_000));
            }
        }
        LocalDateTime localDateTime = TemporalConversions.toLocalDateTime(dbzObj, serverTimeZone);
        return TimestampData.fromLocalDateTime(localDateTime);
    }

    private TimestampData convertToLocalTimeZoneTimestamp(Object dbzObj, Schema schema, Envelope.Operation op) {
        if (dbzObj instanceof String) {
            String str = (String) dbzObj;
            // TIMESTAMP type is encoded in string type
            Instant instant = Instant.parse(str);
            return TimestampData.fromLocalDateTime(LocalDateTime.ofInstant(instant, serverTimeZone));
        }
        throw new IllegalArgumentException("Unable to convert to TimestampData from unexpected value '" + dbzObj + "' of type " + dbzObj.getClass().getName());
    }

    private String convertToString(Object dbzObj, Schema schema, Envelope.Operation op) {
        return dbzObj == null ? null : dbzObj.toString();
    }

    private byte[] convertToBinary(Object dbzObj, Schema schema, Envelope.Operation op) {
        if (dbzObj instanceof byte[]) {
            return (byte[]) dbzObj;
        } else if (dbzObj instanceof ByteBuffer) {
            ByteBuffer byteBuffer = (ByteBuffer) dbzObj;
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            return bytes;
        } else {
            throw new UnsupportedOperationException("Unsupported BYTES value type: " + dbzObj.getClass().getSimpleName());
        }
    }

    private RowDebeziumDeserializeSchema.DeserializationRuntimeConverter createDecimalConverter(DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        return (dbzObj, schema, op, source) -> {
            BigDecimal bigDecimal;
            if (dbzObj instanceof byte[]) {
                // decimal.handling.mode=precise
                bigDecimal = Decimal.toLogical(schema, (byte[]) dbzObj);
            } else if (dbzObj instanceof String) {
                // decimal.handling.mode=string
                bigDecimal = new BigDecimal((String) dbzObj);
            } else if (dbzObj instanceof Double) {
                // decimal.handling.mode=double
                bigDecimal = BigDecimal.valueOf((Double) dbzObj);
            } else {
                if (VariableScaleDecimal.LOGICAL_NAME.equals(schema.name())) {
                    SpecialValueDecimal decimal = VariableScaleDecimal.toLogical((Struct) dbzObj);
                    bigDecimal = decimal.getDecimalValue().orElse(BigDecimal.ZERO);
                } else {
                    // fallback to string
                    bigDecimal = new BigDecimal(dbzObj.toString());
                }
            }
            return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
        };
    }

    private RowDebeziumDeserializeSchema.DeserializationRuntimeConverter createRowConverter(RowType rowType) {
        final RowDebeziumDeserializeSchema.DeserializationRuntimeConverter[] fieldConverters = rowType.getFields().stream()
                .map(RowType.RowField::getType)
                .map(this::createConverter)
                .toArray(RowDebeziumDeserializeSchema.DeserializationRuntimeConverter[]::new);
        final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

        return (dbzObj, schema, op, source) -> {
            Struct struct = (Struct) dbzObj;
            int arity = fieldNames.length;
            Row row = new Row(arity);
            for (int i = 0; i < arity; i++) {
                String fieldName = fieldNames[i];
                if (FIELD_OP.equals(fieldName)) {
                    row.setField(i, op.code());
                } else if (FIELD_SOURCE_DB.equals(fieldName)) {
                    row.setField(i, source.get(FIELD_STRUCT_DB));
                } else if (FIELD_SOURCE_SCHEMA.equals(fieldName)) {
                    row.setField(i, source.get(FIELD_STRUCT_SCHEMA));
                } else if (FIELD_SOURCE_TABLE.equals(fieldName)) {
                    row.setField(i, source.get(FIELD_STRUCT_TABLE));
                } else {
                    Object fieldValue = struct.get(fieldName);
                    Schema fieldSchema = schema.field(fieldName).schema();
                    Object convertedField = convertField(fieldConverters[i], fieldValue, fieldSchema, op, source);
                    row.setField(i, convertedField);
                }
            }
            return row;
        };
    }

    private Object convertField(
            RowDebeziumDeserializeSchema.DeserializationRuntimeConverter fieldConverter,
            Object fieldValue,
            Schema fieldSchema,
            Envelope.Operation op,
            Struct source) throws Exception {
        if (fieldValue == null) {
            return null;
        } else {
            return fieldConverter.convert(fieldValue, fieldSchema, op, source);
        }
    }

    private RowDebeziumDeserializeSchema.DeserializationRuntimeConverter wrapIntoNullableConverter(
            RowDebeziumDeserializeSchema.DeserializationRuntimeConverter converter) {
        return (dbzObj, schema, op, source) -> {
            if (dbzObj == null) {
                return null;
            }
            return converter.convert(dbzObj, schema, op, source);
        };
    }
}
