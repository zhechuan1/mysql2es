
call mvn clean package
copy src\main\resources\mysql2es.properties mysql2es.properties
java -jar -Dfile.encoding=UTF-8  target/mysql2es-1.0-SNAPSHOT.jar
java -Xmx4096m  -Xms4096m -Xmn500m -Dfile.encoding=UTF-8 -jar mysql2es-1.0-SNAPSHOT.jar