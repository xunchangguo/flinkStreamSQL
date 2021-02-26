/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flink.sql.format;

import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.NullNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.TextNode;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;
import java.util.regex.Pattern;

/**
 * source data parse Debezium format
 */
public class DebeziumRowDeserializationSchema extends AbstractDeserializationSchema<Row> {
    private final static String FIELD_TYPE_BYTES = "bytes";
    private final static String KAFKA_CONNECT_DATA_TYPE_DECIMAL = "org.apache.kafka.connect.data.Decimal";
    private final static String PAYLOAD_OP = "payload.op";
    private final static String PAYLOAD_OP_DELETE = "d";
    private final static String PAYLOAD_OP_READ = "r";
    private final static String PAYLOAD_OP_CREATE = "c";
    private final static String PAYLOAD_OP_UPDATE = "u";
    private final static String PAYLOAD_FIELD_AFTER = "payload.after";
    private final static String PAYLOAD_FIELD_BEFORE = "payload.before";
    private final static String FIELD_PREFIX_DATA = "data.";
    private final static String SCHEMA_FIELDS = "schema.fields";
    private final static String FIELD_DATA_SIGN = "data.sign";
    private final static int SIGN_CANCEL = -1;
    private final static int SIGN_ENABLE = 1;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Map<String, String> rowAndFieldMapping;
    private final Map<String, JsonNode> nodeAndJsonNodeMapping = Maps.newHashMap();

    private final String[] fieldNames;
    private final TypeInformation<?>[] fieldTypes;
    private TypeInformation<Row> typeInfo;
    private final List<AbstractTableInfo.FieldExtraInfo> fieldExtraInfos;
    private final String charsetName;
    private boolean collectAll = false;

    private static final Pattern TIMESTAMP_PATTERN = Pattern.compile("^\\d+$");
    private static final Pattern TIME_FORMAT_PATTERN = Pattern.compile("\\w+\\d+:\\d+:\\d+");

    public DebeziumRowDeserializationSchema(TypeInformation<Row> typeInfo, Map<String, String> rowAndFieldMapping,
                                            List<AbstractTableInfo.FieldExtraInfo> fieldExtraInfos,
                                            String charsetName, boolean collectAll) {
        this.typeInfo = typeInfo;
        this.fieldNames = ((RowTypeInfo) typeInfo).getFieldNames();
        this.fieldTypes = ((RowTypeInfo) typeInfo).getFieldTypes();
        this.rowAndFieldMapping = rowAndFieldMapping;
        this.fieldExtraInfos = fieldExtraInfos;
        this.charsetName = charsetName;
        this.collectAll = collectAll;
    }

    @Override
    public Row deserialize(byte[] message) throws IOException {
        String decoderStr = new String(message, charsetName);
        JsonNode root = objectMapper.readTree(decoderStr);
        this.parseTree(root, null);
        return convertTopRow();
    }

    @Override
    public void deserialize(byte[] message, Collector<Row> out) throws IOException {
        if(collectAll) {
            if(message != null) {
                String decoderStr = new String(message, charsetName);
                JsonNode root = objectMapper.readTree(decoderStr);
                this.parseTree(root, null);
                List<Row> rows = convertRow();
                if (rows != null) {
                    for (Row row : rows) {
                        out.collect(row);
                    }
                }
            }
        } else {
            Row deserialize = deserialize(message);
            if (deserialize != null) {
                out.collect(deserialize);
            }
        }
    }

    private void parseTree(JsonNode jsonNode, String prefix) {
        if (jsonNode.isArray()) {
            ArrayNode array = (ArrayNode) jsonNode;
            for (int i = 0; i < array.size(); i++) {
                JsonNode child = array.get(i);
                String nodeKey = getNodeKey(prefix, i);

                if (child.isValueNode()) {
                    nodeAndJsonNodeMapping.put(nodeKey, child);
                } else {
                    if (rowAndFieldMapping.containsValue(nodeKey)) {
                        nodeAndJsonNodeMapping.put(nodeKey, child);
                    }
                    parseTree(child, nodeKey);
                }
            }
            return;
        }
        Iterator<String> iterator = jsonNode.fieldNames();
        while (iterator.hasNext()) {
            String next = iterator.next();
            JsonNode child = jsonNode.get(next);
            String nodeKey = getNodeKey(prefix, next);

            nodeAndJsonNodeMapping.put(nodeKey, child);
            parseTree(child, nodeKey);
        }
    }

    private JsonNode getIgnoreCase(String key, String payloadField) {
        String nodeMappingKey = rowAndFieldMapping.getOrDefault(key, key);
        if(payloadField == null) {
            return nodeAndJsonNodeMapping.get(nodeMappingKey);
        }
        return nodeAndJsonNodeMapping.get(getFieldName(nodeMappingKey, payloadField));
    }

    private String getNodeKey(String prefix, String nodeName) {
        if (Strings.isNullOrEmpty(prefix)) {
            return nodeName;
        }
        return prefix + "." + nodeName;
    }

    private String getNodeKey(String prefix, int i) {
        if (Strings.isNullOrEmpty(prefix)) {
            return "[" + i + "]";
        }
        return prefix + "[" + i + "]";
    }

    private Object convert(JsonNode node, TypeInformation<?> info) {
        if (info.getTypeClass().equals(Types.BOOLEAN.getTypeClass())) {
            return node.asBoolean();
        } else if (info.getTypeClass().equals(Types.STRING.getTypeClass())) {
            if (node instanceof ObjectNode) {
                return node.toString();
            } else if (node instanceof NullNode) {
                return null;
            } else {
                return node.asText();
            }
        } else if (info.getTypeClass().equals(Types.SQL_DATE.getTypeClass())) {
            return Date.valueOf(node.asText());
        } else if (info.getTypeClass().equals(Types.SQL_TIME.getTypeClass())) {
            // local zone
            return convertToTime(node.asText());
        } else if (info.getTypeClass().equals(Types.SQL_TIMESTAMP.getTypeClass())) {
            // local zone
            return convertToTimestamp(node.asText());
        } else if (info instanceof RowTypeInfo) {
            return convertRow(node, (RowTypeInfo) info);
        } else if (info instanceof ObjectArrayTypeInfo) {
            return convertObjectArray(node, ((ObjectArrayTypeInfo) info).getComponentInfo());
        } else {
            // for types that were specified without JSON schema
            // e.g. POJOs
            try {
                return objectMapper.treeToValue(node, info.getTypeClass());
            } catch (JsonProcessingException e) {
                throw new IllegalStateException("Unsupported type information '" + info + "' for node: " + node);
            }
        }
    }

    /**
     * 将 2020-09-07 14:49:10.0 和 1598446699685 两种格式都转化为 Timestamp
     */
    private Timestamp convertToTimestamp(String timestamp) {
        if (TIMESTAMP_PATTERN.matcher(timestamp).find()) {
            return new Timestamp(Long.parseLong(timestamp));
        }
        if (TIME_FORMAT_PATTERN.matcher(timestamp).find()) {
            return Timestamp.valueOf(timestamp);
        }
        throw new IllegalArgumentException("Incorrect time format of timestamp");
    }

    private Time convertToTime(String timestamp) {
        if (TIMESTAMP_PATTERN.matcher(timestamp).find()) {
            return new Time(Long.parseLong(timestamp));
        }
        if (TIME_FORMAT_PATTERN.matcher(timestamp).find()) {
            return Time.valueOf(timestamp);
        }
        throw new IllegalArgumentException("Incorrect time format of time");
    }

    private String getPayloadFieldByOp() {
        JsonNode node = getIgnoreCase(PAYLOAD_OP, null);
        if (node != null) {
            String op = node.asText();
            if(PAYLOAD_OP_DELETE.equals(op)) {
                return PAYLOAD_FIELD_BEFORE;
            }
        }
        return PAYLOAD_FIELD_AFTER;
    }

    private String getFieldName(String fieldName, String payloadField) {
        if(fieldName.startsWith(FIELD_PREFIX_DATA)) {
            return String.format("%s.%s", payloadField, fieldName.substring(FIELD_PREFIX_DATA.length()));
        }
        return fieldName;
    }

    private JsonNode getSchemaNode(String payloadField) {
        JsonNode schema = nodeAndJsonNodeMapping.get(SCHEMA_FIELDS);
        if(schema.isArray()) {
            for (Iterator<JsonNode> it = schema.elements(); it.hasNext(); ) {
                JsonNode sn = it.next();
                if(sn.has("field")){
                    JsonNode fieldNode = sn.get("field");
                    if(payloadField.endsWith(String.format(".%s", fieldNode.asText()))) {
                        return sn.get("fields");
                    }
                }

            }
        }
        return null;
    }

    private JsonNode getValueNode(JsonNode schemaNode, int i, JsonNode valueNode, String fieldName) {
        if(!fieldName.startsWith(FIELD_PREFIX_DATA)) {
            return valueNode;
        }
        if(valueNode.isNull()) {
            return valueNode;
        }
        if(schemaNode != null) {
            try {
                JsonNode fieldNode = schemaNode.get(i);
                if (fieldNode.has("name")) {
                    if (KAFKA_CONNECT_DATA_TYPE_DECIMAL.equals(fieldNode.get("name").asText())) {
                        if (FIELD_TYPE_BYTES.equals(fieldNode.get("type").asText())) {
                            int scale = fieldNode.get("parameters").get("scale").asInt();
                            final BigDecimal decoded = new BigDecimal(new BigInteger(Base64.getDecoder().decode(valueNode.asText())), scale);
                            return new TextNode(decoded.toString());
                        }
                    }
                }
            } catch (Exception e) {
            }
        }
        return valueNode;
    }

    private String getOp() {
        JsonNode node = getIgnoreCase(PAYLOAD_OP, null);
        if (node != null) {
            return node.asText();
        }
        return PAYLOAD_OP_UPDATE;
    }

    private List<Row> convertRow() {
        try {
            String op = getOp();
            if (op.equals(PAYLOAD_OP_CREATE) || op.equals(PAYLOAD_OP_READ)) {
                //after one
                Row row = extractRow(PAYLOAD_FIELD_AFTER, SIGN_ENABLE);
                return Arrays.asList(row);
            } else if (op.equals(PAYLOAD_OP_DELETE)) {
                //before one
                Row row = extractRow(PAYLOAD_FIELD_BEFORE, SIGN_CANCEL);
                return Arrays.asList(row);
            } else {
                //before after
                Row beforeRow = extractRow(PAYLOAD_FIELD_BEFORE, SIGN_CANCEL);
                Row afterRow = extractRow(PAYLOAD_FIELD_AFTER, SIGN_ENABLE);
                return Arrays.asList(beforeRow, afterRow);
            }
        } finally {
            nodeAndJsonNodeMapping.clear();
        }
    }

    private Row extractRow(String payloadField, int sign) {
        Row row = new Row(fieldNames.length);
        JsonNode schemaNode = getSchemaNode(payloadField);
        for (int i = 0; i < fieldNames.length; i++) {
            JsonNode node = getIgnoreCase(fieldNames[i], payloadField);
            String nodeMappingKey = rowAndFieldMapping.getOrDefault(fieldNames[i], fieldNames[i]);
            if(FIELD_DATA_SIGN.equals(nodeMappingKey)) {
                row.setField(i, sign);
                continue;
            }

            AbstractTableInfo.FieldExtraInfo fieldExtraInfo = fieldExtraInfos.get(i);

            if (node == null) {
                if (fieldExtraInfo != null && fieldExtraInfo.getNotNull()) {
                    throw new IllegalStateException("Failed to find field with name '"
                            + fieldNames[i] + "'.");
                } else {
                    row.setField(i, null);
                }
            } else {
                // Read the value as specified type
                Object value = convert(getValueNode(schemaNode, i, node, nodeMappingKey), fieldTypes[i]);
                row.setField(i, value);
            }
        }
        return row;
    }

    private Row convertTopRow() {
        Row row = new Row(fieldNames.length);
        try {
            String payloadField = getPayloadFieldByOp();
            JsonNode schemaNode = getSchemaNode(payloadField);
            for (int i = 0; i < fieldNames.length; i++) {
                JsonNode node = getIgnoreCase(fieldNames[i], payloadField);
                String nodeMappingKey = rowAndFieldMapping.getOrDefault(fieldNames[i], fieldNames[i]);

                AbstractTableInfo.FieldExtraInfo fieldExtraInfo = fieldExtraInfos.get(i);

                if (node == null) {
                    if (fieldExtraInfo != null && fieldExtraInfo.getNotNull()) {
                        throw new IllegalStateException("Failed to find field with name '"
                                + fieldNames[i] + "'.");
                    } else {
                        row.setField(i, null);
                    }
                } else {

                    // Read the value as specified type
                    Object value = convert(getValueNode(schemaNode, i, node, nodeMappingKey), fieldTypes[i]);
                    row.setField(i, value);
                }
            }
            return row;
        } finally {
            nodeAndJsonNodeMapping.clear();
        }
    }

    private Row convertRow(JsonNode node, RowTypeInfo info) {
        final String[] names = info.getFieldNames();
        final TypeInformation<?>[] types = info.getFieldTypes();

        final Row row = new Row(names.length);
        for (int i = 0; i < names.length; i++) {
            final String name = names[i];
            final JsonNode subNode = node.get(name);
            if (subNode == null) {
                row.setField(i, null);
            } else {
                row.setField(i, convert(subNode, types[i]));
            }
        }

        return row;
    }

    private Object convertObjectArray(JsonNode node, TypeInformation<?> elementType) {
        final Object[] array = (Object[]) Array.newInstance(elementType.getTypeClass(), node.size());
        for (int i = 0; i < node.size(); i++) {
            array[i] = convert(node.get(i), elementType);
        }
        return array;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return typeInfo;
    }
}
