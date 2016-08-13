#/bin/bash
export LANG=en_US.UTF-8

cd ..

java -server  -Xms512M -Xmx1024M -XX:PermSize=128M -XX:MaxPermSize=256M -XX:ParallelGCThreads=16 -Xss256k -XX:-DisableExplicitGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled  -jar datasynctohbase.jar
