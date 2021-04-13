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


package com.dtstack.flink.sql.sink.elasticsearch;

import avro.shaded.com.google.common.base.Preconditions;
import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.elasticsearch.table.ElasticsearchTableInfo;
import com.dtstack.flink.sql.table.AbstractTargetTableInfo;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @description:
 * @program: flink.sql
 * @author: lany
 * @create: 2021/01/04 17:20
 */
public class ElasticsearchSink implements RetractStreamTableSink<Row>, IStreamSinkGener<ElasticsearchSink> {

    private final int ES_DEFAULT_PORT = 9200;
    private final String ES_DEFAULT_SCHEMA = "http";

    private int bulkFlushMaxActions = 1;

    private List<String> esAddressList;

    private String index = "";

    private String index_definition;

    private List<String> idFiledNames;

    protected String[] fieldNames;

    protected String[] columnTypes;

    private TypeInformation[] fieldTypes;

    private int parallelism = 1;

    protected String registerTableName;

    private ElasticsearchTableInfo esTableInfo;


    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        return this;
    }

    @Override
    public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
        return new TupleTypeInfo(org.apache.flink.table.api.Types.BOOLEAN(), getRecordType());
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }


    private RichSinkFunction createEsSinkFunction() {

        // check whether id fields is exists in columns
        List<String> filedNamesLists = Arrays.asList(fieldNames);
        if (idFiledNames != null && idFiledNames.size() != 0) {
            Preconditions.checkState(filedNamesLists.containsAll(idFiledNames), "elasticsearch7 type of id %s is should be exists in columns %s.", idFiledNames, filedNamesLists);
        }
        CustomerSinkFunc customerSinkFunc = new CustomerSinkFunc(index, index_definition, Arrays.asList(fieldNames), Arrays.asList(columnTypes), fieldTypes, idFiledNames);


        Map<String, String> userConfig = Maps.newHashMap();
        // This instructs the sink to emit after every element, otherwise they would be buffered
        userConfig.put(org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "" + bulkFlushMaxActions);

        List<HttpHost> transports = esAddressList.stream()
                .map(address -> address.split(":"))
                .map(addressArray -> {
                    String host = addressArray[0].trim();
                    int port = addressArray.length > 1 ? Integer.valueOf(addressArray[1].trim()) : ES_DEFAULT_PORT;
                    return new HttpHost(host.trim(), port, ES_DEFAULT_SCHEMA);
                }).collect(Collectors.toList());
        ExtendEs7ApiCallBridge extendEs7ApiCallBridge = new ExtendEs7ApiCallBridge(transports, esTableInfo);

        return new MetricElasticsearch7Sink(userConfig, extendEs7ApiCallBridge, customerSinkFunc);
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        consumeDataStream(dataStream);
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        RichSinkFunction richSinkFunction = createEsSinkFunction();
        DataStreamSink streamSink = dataStream.addSink(richSinkFunction).name(registerTableName);
        if (parallelism > 0) {
            streamSink.setParallelism(parallelism);
        }
        return streamSink;
    }

    @Override
    public ElasticsearchSink genStreamSink(AbstractTargetTableInfo targetTableInfo) {
        esTableInfo = (ElasticsearchTableInfo) targetTableInfo;
        index = esTableInfo.getIndex();
        index_definition = esTableInfo.getIndex_definition();
        columnTypes = esTableInfo.getFieldTypes();
        esAddressList = Arrays.asList(esTableInfo.getAddress().split(","));
        String id = esTableInfo.getId();
        registerTableName = esTableInfo.getName();
        parallelism = Objects.isNull(esTableInfo.getParallelism()) ? parallelism : esTableInfo.getParallelism();

        if (!StringUtils.isEmpty(id)) {
            idFiledNames = Arrays.stream(StringUtils.split(id, ",")).map(String::valueOf).collect(Collectors.toList());
        }
        return this;
    }

}
