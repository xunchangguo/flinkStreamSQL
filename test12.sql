CREATE TABLE ProductTable (
    id BIGINT,
    productname STRING,
    type INT,
    op STRING
) WITH (
    type = 'postgresqlcdc',
    pluginName = 'pgoutput',
    hostname = '10.88.0.227',
    port = '5432',
    username = 'xxx',
    password = 'xxx',
    database-name = 'postgres',
    table-name = 'public.product',
    schema ='public'
);

CREATE TABLE sink_table (
   ID BIGINT,
   ProductName STRING,
   type int,
   op STRING,
   primary key(ID)  NOT ENFORCED
) WITH (
    type ='mysql',
    url ='jdbc:mysql://127.0.0.1:3306/test?useSSL=false&useUnicode=true&characterEncoding=utf8',
    userName ='xxx',
    password ='xxx',
    tableName ='product',
    parallelism ='1'
);

INSERT OVERWRITE sink_table SELECT id, productname, type, op FROM ProductTable;
