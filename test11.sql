CREATE TABLE ProductTable (
    id BIGINT,
    productname STRING,
    type INT
) WITH (
    type = 'clickhouse',
    url = 'jdbc:clickhouse://10.88.0.227:8123/default',
    username ='',
    password ='',
    tableName ='product'
);

CREATE TABLE sink_table (
   ID BIGINT,
   ProductName STRING,
   type int
) WITH (
    type ='mysql',
    url ='jdbc:mysql://127.0.0.1:3306/test?useSSL=false&useUnicode=true&characterEncoding=utf8',
    userName ='xxx',
    password ='xxx',
    tableName ='product',
    parallelism ='1'
);

INSERT INTO sink_table SELECT id, productname, type FROM ProductTable;
