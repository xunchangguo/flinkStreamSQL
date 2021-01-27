CREATE TABLE ProductTable (
    ID DECIMAL,
    PRODUCTNAME STRING,
    type DECIMAL
) WITH (
    type ='oracle',
    url ='jdbc:oracle:thin:@10.88.0.227:1521:xe',
    username ='xxx',
    password ='xxx',
    tableName ='PRODUCTS'
);

CREATE TABLE sink_table (
   ID DECIMAL,
   ProductName STRING,
   type DECIMAL
) WITH (
    type ='mysql',
    url ='jdbc:mysql://127.0.0.1:3306/test?useSSL=false&useUnicode=true&characterEncoding=utf8',
    userName ='xxx',
    password ='xxx',
    tableName ='product',
    parallelism ='1'
);

INSERT INTO sink_table SELECT ID, PRODUCTNAME, type FROM ProductTable;
