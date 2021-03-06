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


package com.dtstack.flink.sql.source.kafka;

import com.dtstack.flink.sql.source.kafka.table.KafkaSourceTableInfo;
import com.dtstack.flink.sql.table.AbstractSourceTableInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * If eventtime field is specified, the default time field rowtime
 * Date: 2018/09/18
 * Company: www.dtstack.com
 *
 * @author sishu.yss
 */

public class KafkaSource extends AbstractKafkaSource {

    @Override
    public Table genStreamSource(AbstractSourceTableInfo sourceTableInfo, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        KafkaSourceTableInfo kafkaSourceTableInfo = (KafkaSourceTableInfo) sourceTableInfo;
        String topicName = kafkaSourceTableInfo.getTopic();

        Properties kafkaProperties = getKafkaProperties(kafkaSourceTableInfo);
        TypeInformation<Row> typeInformation = getRowTypeInformation(kafkaSourceTableInfo);
        FlinkKafkaConsumer011<Row> kafkaSrc = (FlinkKafkaConsumer011<Row>) new KafkaConsumer011Factory().createKafkaTableSource(kafkaSourceTableInfo, typeInformation, kafkaProperties);

        String sourceOperatorName = generateOperatorName(sourceTableInfo.getName(), topicName);
        DataStreamSource kafkaSource = env.addSource(kafkaSrc, sourceOperatorName, typeInformation);

        setParallelism(kafkaSourceTableInfo.getParallelism(), kafkaSource);
        if (StringUtils.isNotEmpty(kafkaSourceTableInfo.getGroupId())){
            kafkaSrc.setStartFromGroupOffsets();
        } else {
            setStartPosition(kafkaSourceTableInfo.getOffsetReset(), topicName, kafkaSrc, () -> kafkaSrc.setStartFromTimestamp(kafkaSourceTableInfo.getTimestampOffset()));
        }
        String fields = StringUtils.join(kafkaSourceTableInfo.getFields(), ",");

        return tableEnv.fromDataStream(kafkaSource, fields);
    }
}
