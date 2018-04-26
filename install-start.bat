
call mvn clean package
copy src\main\resources\mysql2es.properties mysql2es.properties
java -jar -Dfile.encoding=UTF-8  target/mysql2es-1.0-SNAPSHOT.jar
