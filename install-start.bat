call mvn clean package
copy src\main\resources\mysql2es.properties target\mysql2es.properties
cd target
java -Xmx6144m  -Xms6144m -Xmn500m -Dfile.encoding=UTF-8 -jar mysql2es-1.0-SNAPSHOT.jar mysql2es.properties
pause