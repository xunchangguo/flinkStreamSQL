CREATE TABLE MyUserTable (
  ID BIGINT,
  PRODUCTNAME STRING,
  type INT,
  op STRING,
  primary key(ID)  NOT ENFORCED
) WITH (
    type ='oraclecdc',
    hostname = '10.88.0.227',
    port = '1521',
    username = 'xxx',
    password = 'xxx',
    database-name = 'xe',
    table-name = 'CFSC.PRODUCTS',
    out-server-name = 'dbzxout',
    serverTimeZone = 'UTC',
    database.internal_logon = 'sysdba',
    database.server.id = 'xe'
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
-- INSERT OVERWRITE print_table SELECT ID, ProductName, ProductType, op FROM view_out;
INSERT OVERWRITE print_table SELECT a.ID, a.PRODUCTNAME as ProductName, b.NAME as ProductType, a.op FROM MyUserTable a join ProductTypeDim FOR SYSTEM_TIME AS OF a.PROCTIME AS b on a.type = b.ID;
