CREATE TABLE ProductTable (
    data.ID BIGINT as ID,
    data.PRODUCTNAME STRING as productname,
    data.type INT as type,
    payload.source.ts_ms BIGINT as ts_ms,
    payload.op STRING as op
) WITH (
    type ='kafka',
    bootstrapServers ='10.88.0.227:9093',
    zookeeperQuorum = '10.88.0.227:2181',
    offsetReset ='{"0":14}',
    topic ='server1.CFSC.PRODUCTS',
    sourcedatatype = 'debezium',
    parallelism ='1'
);

CREATE TABLE sink_table (
   ID BIGINT,
   ProductName STRING,
   type int,
   op STRING,
   source_from STRING,
   primary key(ID)  NOT ENFORCED
) WITH (
    type ='mysql',
    url ='jdbc:mysql://127.0.0.1:3306/test?useSSL=false&useUnicode=true&characterEncoding=utf8',
    userName ='xxx',
    password ='xxx',
    tableName ='product',
    parallelism ='1'
);

INSERT OVERWRITE sink_table SELECT ID, productname as ProductName, type, op, CAST(ts_ms AS VARCHAR) AS source_from FROM ProductTable;
