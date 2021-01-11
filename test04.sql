CREATE TABLE MyUserTable (
  ID BIGINT,
  name STRING,
  channel STRING,
  pv BIGINT,
  op STRING,
  primary key(ID)  NOT ENFORCED
) WITH (
    type ='mysqlcdc',
    hostname = 'localhost',
    port = '3306',
    username = 'xxx',
    password = 'xxx',
    database-name = 'test',
    table-name = 'test.test',
    serverTimeZone = 'UTC'
);

CREATE TABLE print_table (
   ID BIGINT,
   name STRING,
   channel STRING,
   pv BIGINT,
   op STRING,
   primary key(ID)  NOT ENFORCED
) WITH (
    type ='mysql',
    url ='jdbc:mysql://127.0.0.1:3306/test?useSSL=false&useUnicode=true&characterEncoding=utf8',
    userName ='xxx',
    password ='xxx',
    tableName ='test_new',
    parallelism ='1'
);

-- scan data from the JDBC table
INSERT OVERWRITE print_table SELECT ID, name, channel, pv, op FROM MyUserTable;
