
call mvn clean package
cd target
java -jar -Dfile.encoding=UTF-8 mysql2es-1.0-SNAPSHOT.jar
