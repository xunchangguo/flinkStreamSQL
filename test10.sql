CREATE TABLE ProductTable (
    id BIGINT,
    productname STRING,
    type INT
) WITH (
    type = 'postgresql',
    url = 'jdbc:postgresql://10.88.0.227:5432/example',
    username ='xxx',
    password ='xxx',
    tableName ='public.product'
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
