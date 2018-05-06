call mvn clean package
copy src\main\resources\mysql2es.properties target\mysql2es.properties
cd target
pause
java -Xmx4096m  -Xms4096m -Xmn500m -Dfile.encoding=UTF-8 -jar mysql2es-1.0-SNAPSHOT.jar mysql2es.properties
pause