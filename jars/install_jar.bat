call mvn install:install-file -DgroupId=com.cloudera.impala.jdbc -DartifactId=ImpalaJDBC41 -Dversion=2.6.12 -Dpackaging=jar -Dfile=ImpalaJDBC4-12.6.12.jar

call mvn install:install-file -DgroupId=com.kingbase8 -DartifactId=kingbase8 -Dversion=8.2.0 -Dpackaging=jar -Dfile=kingbase8-8.2.0.jar

call mvn install:install-file -DgroupId=oracle.streams -DartifactId=xstreams -Dversion=12.2.0.1 -Dpackaging=jar -Dfile=xstreams.jar

call mvn install:install-file -DgroupId=oracle.ojdbc8 -DartifactId=ojdbc8 -Dversion=12.2.0.1 -Dpackaging=jar -Dfile=ojdbc8.jar
