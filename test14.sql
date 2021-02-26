CREATE TABLE ProductTable (
    data.ID BIGINT as id,
    data.PRODUCTNAME STRING as productname,
    data.type INT as type,
    data.sign INT as sign,
    payload.source.ts_ms timestamp as create_time
) WITH (
    type ='kafka',
    bootstrapServers ='10.88.0.227:9093',
    zookeeperQuorum = '10.88.0.227:2181',
    offsetReset ='earliest',
    topic ='server4.CFSC.PRODUCTS',
    sourcedatatype = 'debezium1',
    parallelism ='1'
);

CREATE TABLE sink_table (
   id BIGINT,
   productname STRING,
   type INT,
   Sign INT,
   create_time timestamp,
   primary key(id)  NOT ENFORCED
) WITH (
    type = 'clickhouse',
    url = 'jdbc:clickhouse://10.88.0.227:8123/default?charset=utf8',
    userName ='',
    password ='',
    tableName ='productCol',
    updateMode = 'append',
    parallelism ='1'
);

INSERT OVERWRITE sink_table SELECT id, productname, type, sign as Sign, create_time FROM ProductTable;