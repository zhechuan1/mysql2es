
call mvn clean package
java -jar -Dfile.encoding=UTF-8  target/mysql2es-1.0-SNAPSHOT.jar F:\output\intellij\mysql2es\mysql2es.properties
