call mvn clean package
copy src\main\resources\mysql2es.properties target\mysql2es.properties
copy start.bat target\start.bat
cd target
call ./start.bat