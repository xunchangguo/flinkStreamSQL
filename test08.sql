CREATE TABLE ProductTable (
  DesktopGUID STRING,
  IsPublic BOOLEAN,
  Owner STRING
) WITH (
    type ='sqlserver',
    url ='jdbc:jtds:sqlserver://10.10.88.12:1433;DatabaseName=xxx',
    username ='xxx',
    password ='xxx',
    tableName ='NewDesktop'
);

CREATE TABLE sink_table (
   DesktopGUID STRING,
   IsPublic BOOLEAN,
   Owner STRING
) WITH (
    type ='mysql',
    url ='jdbc:mysql://127.0.0.1:3306/test?useSSL=false&useUnicode=true&characterEncoding=utf8',
    userName ='xxx',
    password ='xxx',
    tableName ='NewDesktop',
    parallelism ='1'
);

INSERT INTO sink_table SELECT DesktopGUID, IsPublic, Owner FROM ProductTable;
