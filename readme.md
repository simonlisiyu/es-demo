> mvn clean package

#### 参数依次是 index, type, filename
> jdk1.8.0_111/bin/java -cp ./es-demo-1.0-SNAPSHOT.jar com.jusfoun.es03.EsOutput mjdos log mjdos.bak

> jdk1.8.0_111/bin/java -cp ./es-demo-1.0-SNAPSHOT.jar com.jusfoun.es03.EsOutputTimeRange mjdos log mjdos.bak 1487570100000 1487257200000

> jdk1.8.0_111/bin/java -cp ./es-demo-1.0-SNAPSHOT.jar com.jusfoun.es03.EsImport2 mjdos log mjdos.bak

