CREATE TABLE MyUserTable (
  ID BIGINT,
  ProductName STRING,
  type INT,
  op STRING,
  primary key(ID)  NOT ENFORCED
) WITH (
    type ='sqlservercdc',
    hostname = '10.88.0.227',
    port = '1433',
    username = 'xxx',
    password = 'xxx',
    database-name = 'DemoData',
    table-name = 'dbo.Products',
    serverTimeZone = 'UTC'
);

CREATE TABLE ProductTypeDim (
  ID INT,
  NAME STRING,
  PRIMARY KEY(ID),
  PERIOD FOR SYSTEM_TIME
) WITH (
    type ='mysql',
    url ='jdbc:mysql://127.0.0.1:3306/test?useSSL=false&useUnicode=true&characterEncoding=utf8',
    userName ='xxx',
    password ='xxx',
    tableName ='product_type',
    parallelism ='1'
);

CREATE TABLE print_table (
   ID BIGINT,
   ProductName STRING,
   ProductType STRING,
   op STRING,
   primary key(ID)  NOT ENFORCED
) WITH (
    type ='mysql',
    url ='jdbc:mysql://127.0.0.1:3306/test?useSSL=false&useUnicode=true&characterEncoding=utf8',
    userName ='xxx',
    password ='xxx',
    tableName ='product_result',
    parallelism ='1'
);

-- CREATE VIEW view_out AS SELECT a.ID, a.ProductName, b.NAME as ProductType, a.op FROM MyUserTable a join ProductTypeDim FOR SYSTEM_TIME AS OF a.PROCTIME AS b on a.type = b.ID;
-- scan data from the JDBC table
-- INSERT OVERWRITE print_table SELECT ID, ProductName, ProductType, op FROM view_out;
INSERT OVERWRITE print_table SELECT a.ID, a.ProductName, b.NAME as ProductType, a.op FROM MyUserTable a join ProductTypeDim FOR SYSTEM_TIME AS OF a.PROCTIME AS b on a.type = b.ID;
