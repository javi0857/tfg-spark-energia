file:///C:/Proyectos/spark-datos-energia/src/main/scala/example/RegionesBalanceDownloader.scala
### java.lang.IndexOutOfBoundsException: -1 is out of bounds (min 0, max 2)

occurred in the presentation compiler.

presentation compiler configuration:
Scala version: 2.13.15
Classpath:
<WORKSPACE>\.bloop\root\bloop-bsp-clients-classes\classes-Metals-_ZZnu5wlQ4yrRgNqSvveug== [exists ], <HOME>\AppData\Local\bloop\cache\semanticdb\com.sourcegraph.semanticdb-javac.0.10.3\semanticdb-javac-0.10.3.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala-library\2.13.15\scala-library-2.13.15.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-core_2.13\3.5.0\spark-core_2.13-3.5.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-sql_2.13\3.5.0\spark-sql_2.13-3.5.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-common\3.3.6\hadoop-common-3.3.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-client\3.3.6\hadoop-client-3.3.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scalameta\munit_2.13\0.7.29\munit_2.13-0.7.29.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\softwaremill\sttp\client4\core_2.13\4.0.0-M19\core_2.13-4.0.0-M19.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\softwaremill\sttp\client4\async-http-client-backend-future_2.13\4.0.0-M19\async-http-client-backend-future_2.13-4.0.0-M19.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\softwaremill\sttp\client4\circe_2.13\4.0.0-M19\circe_2.13-4.0.0-M19.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\circe\circe-generic_2.13\0.14.1\circe-generic_2.13-0.14.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\circe\circe-parser_2.13\0.14.10\circe-parser_2.13-0.14.10.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\modules\scala-parallel-collections_2.13\1.0.4\scala-parallel-collections_2.13-1.0.4.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\avro\avro\1.11.2\avro-1.11.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\avro\avro-mapred\1.11.2\avro-mapred-1.11.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\twitter\chill_2.13\0.10.0\chill_2.13-0.10.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\twitter\chill-java\0.10.0\chill-java-0.10.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\xbean\xbean-asm9-shaded\4.23\xbean-asm9-shaded-4.23.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-client-api\3.3.4\hadoop-client-api-3.3.4.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-client-runtime\3.3.4\hadoop-client-runtime-3.3.4.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-launcher_2.13\3.5.0\spark-launcher_2.13-3.5.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-kvstore_2.13\3.5.0\spark-kvstore_2.13-3.5.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-network-common_2.13\3.5.0\spark-network-common_2.13-3.5.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-network-shuffle_2.13\3.5.0\spark-network-shuffle_2.13-3.5.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-unsafe_2.13\3.5.0\spark-unsafe_2.13-3.5.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-common-utils_2.13\3.5.0\spark-common-utils_2.13-3.5.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\javax\activation\activation\1.1.1\activation-1.1.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\curator\curator-recipes\5.2.0\curator-recipes-5.2.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\zookeeper\zookeeper\3.6.3\zookeeper-3.6.3.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\jakarta\servlet\jakarta.servlet-api\4.0.3\jakarta.servlet-api-4.0.3.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\commons-codec\commons-codec\1.16.0\commons-codec-1.16.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\commons\commons-compress\1.23.0\commons-compress-1.23.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\commons\commons-lang3\3.12.0\commons-lang3-3.12.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\commons\commons-math3\3.6.1\commons-math3-3.6.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\commons\commons-text\1.10.0\commons-text-1.10.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\commons-io\commons-io\2.13.0\commons-io-2.13.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\commons-collections\commons-collections\3.2.2\commons-collections-3.2.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\commons\commons-collections4\4.4\commons-collections4-4.4.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\google\code\findbugs\jsr305\3.0.2\jsr305-3.0.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\ning\compress-lzf\1.1.2\compress-lzf-1.1.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\xerial\snappy\snappy-java\1.1.10.3\snappy-java-1.1.10.3.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\lz4\lz4-java\1.8.0\lz4-java-1.8.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\github\luben\zstd-jni\1.5.5-4\zstd-jni-1.5.5-4.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\roaringbitmap\RoaringBitmap\0.9.45\RoaringBitmap-0.9.45.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\modules\scala-xml_2.13\2.1.0\scala-xml_2.13-2.1.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala-reflect\2.13.15\scala-reflect-2.13.15.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\json4s\json4s-jackson_2.13\3.7.0-M11\json4s-jackson_2.13-3.7.0-M11.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\jersey\core\jersey-client\2.40\jersey-client-2.40.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\jersey\core\jersey-common\2.40\jersey-common-2.40.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\jersey\core\jersey-server\2.40\jersey-server-2.40.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\jersey\containers\jersey-container-servlet\2.40\jersey-container-servlet-2.40.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\jersey\containers\jersey-container-servlet-core\2.40\jersey-container-servlet-core-2.40.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\jersey\inject\jersey-hk2\2.40\jersey-hk2-2.40.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-all\4.1.96.Final\netty-all-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-transport-native-epoll\4.1.96.Final\netty-transport-native-epoll-4.1.96.Final-linux-x86_64.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-transport-native-epoll\4.1.96.Final\netty-transport-native-epoll-4.1.96.Final-linux-aarch_64.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-transport-native-kqueue\4.1.96.Final\netty-transport-native-kqueue-4.1.96.Final-osx-aarch_64.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-transport-native-kqueue\4.1.96.Final\netty-transport-native-kqueue-4.1.96.Final-osx-x86_64.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\clearspring\analytics\stream\2.9.6\stream-2.9.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\dropwizard\metrics\metrics-core\4.2.19\metrics-core-4.2.19.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\dropwizard\metrics\metrics-jvm\4.2.19\metrics-jvm-4.2.19.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\dropwizard\metrics\metrics-json\4.2.19\metrics-json-4.2.19.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\dropwizard\metrics\metrics-graphite\4.2.19\metrics-graphite-4.2.19.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\dropwizard\metrics\metrics-jmx\4.2.19\metrics-jmx-4.2.19.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\fasterxml\jackson\core\jackson-databind\2.15.2\jackson-databind-2.15.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\fasterxml\jackson\module\jackson-module-scala_2.13\2.15.2\jackson-module-scala_2.13-2.15.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\ivy\ivy\2.5.1\ivy-2.5.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\oro\oro\2.0.8\oro-2.0.8.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\net\razorvine\pickle\1.3\pickle-1.3.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\net\sf\py4j\py4j\0.10.9.7\py4j-0.10.9.7.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-tags_2.13\3.5.0\spark-tags_2.13-3.5.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\commons\commons-crypto\1.1.0\commons-crypto-1.1.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\rocksdb\rocksdbjni\8.3.2\rocksdbjni-8.3.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\univocity\univocity-parsers\2.9.1\univocity-parsers-2.9.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-sketch_2.13\3.5.0\spark-sketch_2.13-3.5.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-catalyst_2.13\3.5.0\spark-catalyst_2.13-3.5.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\orc\orc-core\1.9.1\orc-core-1.9.1-shaded-protobuf.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\orc\orc-mapreduce\1.9.1\orc-mapreduce-1.9.1-shaded-protobuf.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hive\hive-storage-api\2.8.1\hive-storage-api-2.8.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\parquet\parquet-column\1.13.1\parquet-column-1.13.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\parquet\parquet-hadoop\1.13.1\parquet-hadoop-1.13.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\thirdparty\hadoop-shaded-protobuf_3_7\1.1.1\hadoop-shaded-protobuf_3_7-1.1.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-annotations\3.3.6\hadoop-annotations-3.3.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\thirdparty\hadoop-shaded-guava\1.1.1\hadoop-shaded-guava-1.1.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\google\guava\guava\27.0.1-jre\guava-27.0.1-jre.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\commons-cli\commons-cli\1.2\commons-cli-1.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\httpcomponents\httpclient\4.5.13\httpclient-4.5.13.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\commons-net\commons-net\3.9.0\commons-net-3.9.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\javax\servlet\javax.servlet-api\3.1.0\javax.servlet-api-3.1.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\jakarta\activation\jakarta.activation-api\1.2.1\jakarta.activation-api-1.2.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\eclipse\jetty\jetty-server\9.4.51.v20230217\jetty-server-9.4.51.v20230217.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\eclipse\jetty\jetty-util\9.4.51.v20230217\jetty-util-9.4.51.v20230217.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\eclipse\jetty\jetty-servlet\9.4.51.v20230217\jetty-servlet-9.4.51.v20230217.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\eclipse\jetty\jetty-webapp\9.4.51.v20230217\jetty-webapp-9.4.51.v20230217.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\javax\servlet\jsp\jsp-api\2.1\jsp-api-2.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\sun\jersey\jersey-core\1.19.4\jersey-core-1.19.4.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\sun\jersey\jersey-servlet\1.19.4\jersey-servlet-1.19.4.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\github\pjfanning\jersey-json\1.20\jersey-json-1.20.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\sun\jersey\jersey-server\1.19.4\jersey-server-1.19.4.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\commons-logging\commons-logging\1.2\commons-logging-1.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\ch\qos\reload4j\reload4j\1.2.22\reload4j-1.2.22.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\commons-beanutils\commons-beanutils\1.9.4\commons-beanutils-1.9.4.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\commons\commons-configuration2\2.8.0\commons-configuration2-2.8.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\slf4j\slf4j-api\2.0.7\slf4j-api-2.0.7.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\slf4j\slf4j-reload4j\1.7.36\slf4j-reload4j-1.7.36.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\google\re2j\re2j\1.1\re2j-1.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\google\protobuf\protobuf-java\3.19.6\protobuf-java-3.19.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\google\code\gson\gson\2.10.1\gson-2.10.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-auth\3.3.6\hadoop-auth-3.3.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\jcraft\jsch\0.1.55\jsch-0.1.55.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\curator\curator-client\5.2.0\curator-client-5.2.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\kerby\kerb-core\1.0.1\kerb-core-1.0.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\codehaus\woodstox\stax2-api\4.2.1\stax2-api-4.2.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\fasterxml\woodstox\woodstox-core\5.4.0\woodstox-core-5.4.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\dnsjava\dnsjava\2.1.7\dnsjava-2.1.7.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-hdfs-client\3.3.6\hadoop-hdfs-client-3.3.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-yarn-api\3.3.6\hadoop-yarn-api-3.3.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-yarn-client\3.3.6\hadoop-yarn-client-3.3.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-mapreduce-client-core\3.3.6\hadoop-mapreduce-client-core-3.3.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-mapreduce-client-jobclient\3.3.6\hadoop-mapreduce-client-jobclient-3.3.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scalameta\junit-interface\0.7.29\junit-interface-0.7.29.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\junit\junit\4.13.2\junit-4.13.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\softwaremill\sttp\model\core_2.13\1.7.11\core_2.13-1.7.11.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\softwaremill\sttp\shared\core_2.13\1.3.22\core_2.13-1.3.22.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\softwaremill\sttp\shared\ws_2.13\1.3.22\ws_2.13-1.3.22.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\softwaremill\sttp\client4\async-http-client-backend_2.13\4.0.0-M19\async-http-client-backend_2.13-4.0.0-M19.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\softwaremill\sttp\client4\json-common_2.13\4.0.0-M19\json-common_2.13-4.0.0-M19.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\circe\circe-core_2.13\0.14.10\circe-core_2.13-0.14.10.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\chuusai\shapeless_2.13\2.3.7\shapeless_2.13-2.3.7.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\circe\circe-jawn_2.13\0.14.10\circe-jawn_2.13-0.14.10.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\fasterxml\jackson\core\jackson-core\2.15.2\jackson-core-2.15.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\avro\avro-ipc\1.11.2\avro-ipc-1.11.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\esotericsoftware\kryo-shaded\4.0.2\kryo-shaded-4.0.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\fusesource\leveldbjni\leveldbjni-all\1.8\leveldbjni-all-1.8.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\fasterxml\jackson\core\jackson-annotations\2.15.2\jackson-annotations-2.15.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\google\crypto\tink\tink\1.9.0\tink-1.9.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\slf4j\jul-to-slf4j\2.0.7\jul-to-slf4j-2.0.7.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\slf4j\jcl-over-slf4j\2.0.7\jcl-over-slf4j-2.0.7.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\logging\log4j\log4j-slf4j2-impl\2.20.0\log4j-slf4j2-impl-2.20.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\logging\log4j\log4j-api\2.20.0\log4j-api-2.20.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\logging\log4j\log4j-core\2.20.0\log4j-core-2.20.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\logging\log4j\log4j-1.2-api\2.20.0\log4j-1.2-api-2.20.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\curator\curator-framework\5.2.0\curator-framework-5.2.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\zookeeper\zookeeper-jute\3.6.3\zookeeper-jute-3.6.3.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\yetus\audience-annotations\0.13.0\audience-annotations-0.13.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\roaringbitmap\shims\0.9.45\shims-0.9.45.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\json4s\json4s-core_2.13\3.7.0-M11\json4s-core_2.13-3.7.0-M11.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\jakarta\ws\rs\jakarta.ws.rs-api\2.1.6\jakarta.ws.rs-api-2.1.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\hk2\external\jakarta.inject\2.6.1\jakarta.inject-2.6.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\jakarta\annotation\jakarta.annotation-api\1.3.5\jakarta.annotation-api-1.3.5.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\hk2\osgi-resource-locator\1.0.3\osgi-resource-locator-1.0.3.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\jakarta\validation\jakarta.validation-api\2.0.2\jakarta.validation-api-2.0.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\hk2\hk2-locator\2.6.1\hk2-locator-2.6.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\javassist\javassist\3.29.2-GA\javassist-3.29.2-GA.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-buffer\4.1.96.Final\netty-buffer-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-codec\4.1.96.Final\netty-codec-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-codec-http\4.1.96.Final\netty-codec-http-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-codec-http2\4.1.96.Final\netty-codec-http2-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-codec-socks\4.1.96.Final\netty-codec-socks-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-common\4.1.96.Final\netty-common-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-handler\4.1.96.Final\netty-handler-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-transport-native-unix-common\4.1.96.Final\netty-transport-native-unix-common-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-handler-proxy\4.1.96.Final\netty-handler-proxy-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-resolver\4.1.96.Final\netty-resolver-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-transport\4.1.96.Final\netty-transport-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-transport-classes-epoll\4.1.96.Final\netty-transport-classes-epoll-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-transport-classes-kqueue\4.1.96.Final\netty-transport-classes-kqueue-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\thoughtworks\paranamer\paranamer\2.8\paranamer-2.8.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-sql-api_2.13\3.5.0\spark-sql-api_2.13-3.5.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\codehaus\janino\janino\3.1.9\janino-3.1.9.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\codehaus\janino\commons-compiler\3.1.9\commons-compiler-3.1.9.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\datasketches\datasketches-java\3.3.0\datasketches-java-3.3.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\orc\orc-shims\1.9.1\orc-shims-1.9.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\airlift\aircompressor\0.25\aircompressor-0.25.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\jetbrains\annotations\17.0.0\annotations-17.0.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\threeten\threeten-extra\1.7.1\threeten-extra-1.7.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\parquet\parquet-common\1.13.1\parquet-common-1.13.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\parquet\parquet-encoding\1.13.1\parquet-encoding-1.13.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\parquet\parquet-format-structures\1.13.1\parquet-format-structures-1.13.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\parquet\parquet-jackson\1.13.1\parquet-jackson-1.13.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\google\guava\failureaccess\1.0.1\failureaccess-1.0.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\google\guava\listenablefuture\9999.0-empty-to-avoid-conflict-with-guava\listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\checkerframework\checker-qual\2.5.2\checker-qual-2.5.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\google\j2objc\j2objc-annotations\1.1\j2objc-annotations-1.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\codehaus\mojo\animal-sniffer-annotations\1.17\animal-sniffer-annotations-1.17.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\httpcomponents\httpcore\4.4.13\httpcore-4.4.13.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\eclipse\jetty\jetty-http\9.4.51.v20230217\jetty-http-9.4.51.v20230217.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\eclipse\jetty\jetty-io\9.4.51.v20230217\jetty-io-9.4.51.v20230217.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\eclipse\jetty\jetty-security\9.4.51.v20230217\jetty-security-9.4.51.v20230217.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\eclipse\jetty\jetty-util-ajax\9.4.51.v20230217\jetty-util-ajax-9.4.51.v20230217.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\eclipse\jetty\jetty-xml\9.4.51.v20230217\jetty-xml-9.4.51.v20230217.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\javax\ws\rs\jsr311-api\1.1.1\jsr311-api-1.1.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\codehaus\jettison\jettison\1.1\jettison-1.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\sun\xml\bind\jaxb-impl\2.2.3-1\jaxb-impl-2.2.3-1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\nimbusds\nimbus-jose-jwt\9.8.1\nimbus-jose-jwt-9.8.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\kerby\kerb-simplekdc\1.0.1\kerb-simplekdc-1.0.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-transport-native-epoll\4.1.96.Final\netty-transport-native-epoll-4.1.96.Final.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\slf4j\slf4j-log4j12\1.7.25\slf4j-log4j12-1.7.25.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\log4j\log4j\1.2.17\log4j-1.2.17.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\kerby\kerby-pkix\1.0.1\kerby-pkix-1.0.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\javax\xml\bind\jaxb-api\2.2.11\jaxb-api-2.2.11.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\eclipse\jetty\websocket\websocket-client\9.4.51.v20230217\websocket-client-9.4.51.v20230217.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\jline\jline\3.9.0\jline-3.9.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-yarn-common\3.3.6\hadoop-yarn-common-3.3.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-mapreduce-client-common\3.3.6\hadoop-mapreduce-client-common-3.3.6.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-sbt\test-interface\1.0\test-interface-1.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\hamcrest\hamcrest-core\1.3\hamcrest-core-1.3.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\asynchttpclient\async-http-client\2.12.3\async-http-client-2.12.3.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\circe\circe-numbers_2.13\0.14.10\circe-numbers_2.13-0.14.10.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\typelevel\cats-core_2.13\2.12.0\cats-core_2.13-2.12.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\typelevel\jawn-parser_2.13\1.6.0\jawn-parser_2.13-1.6.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\tukaani\xz\1.9\xz-1.9.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\esotericsoftware\minlog\1.3.0\minlog-1.3.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\objenesis\objenesis\2.5.1\objenesis-2.5.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\joda-time\joda-time\2.12.5\joda-time-2.12.5.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\json4s\json4s-ast_2.13\3.7.0-M11\json4s-ast_2.13-3.7.0-M11.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\json4s\json4s-scalap_2.13\3.7.0-M11\json4s-scalap_2.13-3.7.0-M11.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\hk2\external\aopalliance-repackaged\2.6.1\aopalliance-repackaged-2.6.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\hk2\hk2-api\2.6.1\hk2-api-2.6.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\hk2\hk2-utils\2.6.1\hk2-utils-2.6.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\modules\scala-parser-combinators_2.13\2.3.0\scala-parser-combinators_2.13-2.3.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\antlr\antlr4-runtime\4.9.3\antlr4-runtime-4.9.3.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\arrow\arrow-vector\12.0.1\arrow-vector-12.0.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\arrow\arrow-memory-netty\12.0.1\arrow-memory-netty-12.0.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\datasketches\datasketches-memory\2.1.0\datasketches-memory-2.1.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\github\stephenc\jcip\jcip-annotations\1.0-1\jcip-annotations-1.0-1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\kerby\kerb-client\1.0.1\kerb-client-1.0.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\kerby\kerb-admin\1.0.1\kerb-admin-1.0.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\kerby\kerby-asn1\1.0.1\kerby-asn1-1.0.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\kerby\kerby-util\1.0.1\kerby-util-1.0.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\eclipse\jetty\jetty-client\9.4.51.v20230217\jetty-client-9.4.51.v20230217.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\eclipse\jetty\websocket\websocket-common\9.4.51.v20230217\websocket-common-9.4.51.v20230217.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\sun\jersey\jersey-client\1.19.4\jersey-client-1.19.4.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\fasterxml\jackson\module\jackson-module-jaxb-annotations\2.12.7\jackson-module-jaxb-annotations-2.12.7.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\fasterxml\jackson\jaxrs\jackson-jaxrs-json-provider\2.12.7\jackson-jaxrs-json-provider-2.12.7.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\squareup\okhttp3\okhttp\4.9.3\okhttp-4.9.3.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\jetbrains\kotlin\kotlin-stdlib\1.4.10\kotlin-stdlib-1.4.10.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\jetbrains\kotlin\kotlin-stdlib-common\1.4.10\kotlin-stdlib-common-1.4.10.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\asynchttpclient\async-http-client-netty-utils\2.12.3\async-http-client-netty-utils-2.12.3.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\reactivestreams\reactive-streams\1.0.3\reactive-streams-1.0.3.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\typesafe\netty\netty-reactive-streams\2.0.4\netty-reactive-streams-2.0.4.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\sun\activation\jakarta.activation\1.2.2\jakarta.activation-1.2.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\typelevel\cats-kernel_2.13\2.12.0\cats-kernel_2.13-2.12.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\arrow\arrow-format\12.0.1\arrow-format-12.0.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\arrow\arrow-memory-core\12.0.1\arrow-memory-core-12.0.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\fasterxml\jackson\datatype\jackson-datatype-jsr310\2.15.1\jackson-datatype-jsr310-2.15.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\google\flatbuffers\flatbuffers-java\1.12.0\flatbuffers-java-1.12.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\kerby\kerby-config\1.0.1\kerby-config-1.0.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\kerby\kerb-common\1.0.1\kerb-common-1.0.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\kerby\kerb-util\1.0.1\kerb-util-1.0.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\kerby\token-provider\1.0.1\token-provider-1.0.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\kerby\kerb-server\1.0.1\kerb-server-1.0.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\kerby\kerby-xdr\1.0.1\kerby-xdr-1.0.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\eclipse\jetty\websocket\websocket-api\9.4.51.v20230217\websocket-api-9.4.51.v20230217.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\jakarta\xml\bind\jakarta.xml.bind-api\2.3.2\jakarta.xml.bind-api-2.3.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\fasterxml\jackson\jaxrs\jackson-jaxrs-base\2.12.7\jackson-jaxrs-base-2.12.7.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\squareup\okio\okio\2.8.0\okio-2.8.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\google\inject\guice\4.0\guice-4.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\sun\jersey\contribs\jersey-guice\1.19.4\jersey-guice-1.19.4.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\google\errorprone\error_prone_annotations\2.2.0\error_prone_annotations-2.2.0.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\kerby\kerb-crypto\1.0.1\kerb-crypto-1.0.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\kerby\kerb-identity\1.0.1\kerb-identity-1.0.1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\javax\inject\javax.inject\1\javax.inject-1.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\aopalliance\aopalliance\1.0\aopalliance-1.0.jar [exists ]
Options:
-Yrangepos -Xplugin-require:semanticdb -release 11


action parameters:
uri: file:///C:/Proyectos/spark-datos-energia/src/main/scala/example/RegionesBalanceDownloader.scala
text:
```scala
package example

import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.duration._


import org.apache.spark.sql.{DataFrame,SparkSession}
import org.apache.spark.sql.functions._

import example.Utils._
import cats.instances.boolean
import org.apache.spark.sql.Dataset


//Defino la clase RegionData para crear el dataset con los id de las regiones
case class RegionData (
            region: String, 
            geo_limit: String,
            geo_id: String
        )

object RegionsBalanceDownoloader {
    def main(args: Array[String]): Unit = {
    
        println("EMPEZAMOS EJECUCIÓN")

        // Crear sesión de Spark
        val spark = SparkSession.builder()
                .appName("BalanceDownloader")
                .master("local[*]")
                .getOrCreate() 

        import spark.implicits._

        //Leemos información con los id de las regiones

        val filePath = "data/csv/regionDataAux.csv"         
        val dsRegion: Dataset[RegionData] = spark.read
            .option("header", "true") // Si el archivo tiene un encabezado
            .option("inferSchema", "true") // Para inferir automáticamente el esquema
            .csv(filePath)
            .as[RegionData]


        val start = "2011-01-01T00:00" //Tenemos datos de Balance desde 2011-01-01
        val end = "2024-12-31T23:59"
        val interval = "year" //Al ser datos diarios podemos solicitar un año entero con cada llamada a la api

        //Recorremos la lista de regiones y creamos una dupla (Region, listResponse) por cada fila

        val listRegiones = dsRegion.
        val listRegiones = dsRegion.map { aux => 

            val region = aux.region //Guardamos la región sobre la que vamos a operar
            val listResponses = callApiBalance(start, end, interval, aux.geo_limit, aux.geo_id)
            val listModel = listResponses.map {
                    response => transformToBalanceModel(responseToDF(response)(spark))(spark)
                }
            val model = listModel.reduce(_ union _)
            model.withColumn("Region", lit(region))
        } 

        listRegiones

        spark.stop()

        println("FINAL DE LA EJECUCIÓN")


    }

    




   def callApiBalance (start: String, end: String, interval: String, geoLimit: String, geoIds: String): Seq[Right[Nothing,String]] = {

        // Doy valor a los parámetros para llamar a la API
        val category = "balance"
        val widget = "balance-electrico"
        val time_trunc = "day"
        val lang = "es"
        val geo_trunc = "electric_system"
        val geo_limit = geoLimit
        val geo_ids = geoIds

        // Creamos secuencia de parejas (String, String) segun las fechas que le hemos dado y el intervalo
        val rangoFechas = buildDateRange(start, end , interval) 

        // Creamos una uri para cada pareja
        val listauris = rangoFechas.map { 
            case (start, end) => createUri(category, widget, start, end, time_trunc, lang, geo_trunc, geo_limit, geo_ids)
        } 

        // Imprimir uris generadas
        listauris.foreach(println(_))

        // Registrar el tiempo inicial
        val startTime = System.nanoTime()  

        // Hacemos la llamada a la Api creando Futures para hacerlo de forma concurrente
        val futureResponses = listauris.map { uri =>
            Future {
                getApiData(uri) match {
                case Right(response) => Right(response)
                case Left(error) => throw new Exception(error)
                }
            }
        }

        // Utilizar Future.sequence para esperar a que todos los Futures se completen
        val combinedFuture = Future.sequence(futureResponses)

        
        // Utilizar Await para bloquear hasta que todos los Futures se completen (para no terminar el programa)
        val listResponses = Await.result(combinedFuture, Duration.Inf)
        
        // Registrar el tiempo final y mostrar la duración
        val endTimeConcurrent = System.nanoTime()
        val totalTime = (endTimeConcurrent - startTime) / 1e9 // Convertir a segundos

        println(s"TIEMPO TOTAL DE EJECUCIÓN DE LLAMADA A LA API: $totalTime segundos")

        listResponses //Devolvemos la lista con los responses
    }

  def transformToBalanceModel(df: DataFrame)(implicit spark: SparkSession): DataFrame = {

        // Importar implicits para poder usar $"columnName"
        import spark.implicits._

        // Definir los tipos de energía de bajas emisiones
        val bajasEmisiones = Seq(
            "Solar fotovoltaica", 
            "Solar térmica", 
            "Nuclear", 
            "Hidroeólica", 
            "Eólica", 
            "Generación renovable", 
            "Otras renovables",
            "Hidráulica", 
            "Residuos renovables"
        )

        //Crear modelo desanidado
        df.withColumn("FamilyGroup", explode($"included"))
            .withColumn("TypeGroup", explode($"FamilyGroup.attributes.content"))
            .withColumn("Values", explode($"TypeGroup.attributes.values"))
            .select(
                $"FamilyGroup.type".as("Familia"),
                $"TypeGroup.type".as("Tipo"),
                $"TypeGroup.attributes.composite".as("Compuesto"),
                $"Values.datetime".as("FechaCompleta"), 
                $"Values.percentage".as("Porcentaje"),
                $"Values.value".as("Valor")
            )
            .withColumn("FechaCompleta", $"FechaCompleta".cast("timestamp"))
            .withColumn("BajasEmisiones", $"Tipo".isin(bajasEmisiones: _*))
    }
}
```



#### Error stacktrace:

```
scala.collection.mutable.ArrayBuffer.apply(ArrayBuffer.scala:102)
	scala.reflect.internal.Types$Type.findMemberInternal$1(Types.scala:1030)
	scala.reflect.internal.Types$Type.findMember(Types.scala:1035)
	scala.reflect.internal.Types$Type.memberBasedOnName(Types.scala:661)
	scala.reflect.internal.Types$Type.member(Types.scala:625)
	scala.tools.nsc.typechecker.Contexts$SymbolLookup.nextDefinition$1(Contexts.scala:1447)
	scala.tools.nsc.typechecker.Contexts$SymbolLookup.apply(Contexts.scala:1521)
	scala.tools.nsc.typechecker.Contexts$Context.lookupSymbol(Contexts.scala:1287)
	scala.tools.nsc.typechecker.Typers$Typer.typedIdent$2(Typers.scala:5695)
	scala.tools.nsc.typechecker.Typers$Typer.typedIdentOrWildcard$1(Typers.scala:5789)
	scala.tools.nsc.typechecker.Typers$Typer.typed1(Typers.scala:6262)
	scala.tools.nsc.typechecker.Typers$Typer.typed(Typers.scala:6320)
	scala.tools.nsc.typechecker.Typers$Typer.$anonfun$typed1$41(Typers.scala:5238)
	scala.tools.nsc.typechecker.Typers$Typer.silent(Typers.scala:713)
	scala.tools.nsc.typechecker.Typers$Typer.normalTypedApply$1(Typers.scala:5240)
	scala.tools.nsc.typechecker.Typers$Typer.typedApply$1(Typers.scala:5272)
	scala.tools.nsc.typechecker.Typers$Typer.typed1(Typers.scala:6264)
	scala.tools.nsc.typechecker.Typers$Typer.typed(Typers.scala:6320)
	scala.tools.nsc.typechecker.Typers$Typer.typedCase(Typers.scala:6409)
	scala.tools.nsc.typechecker.Typers$Typer.$anonfun$typedCases$1(Typers.scala:2710)
	scala.tools.nsc.typechecker.Typers$Typer.typedCases(Typers.scala:2709)
	scala.tools.nsc.typechecker.Typers$Typer.typedMatch(Typers.scala:2721)
	scala.tools.nsc.typechecker.Typers$Typer.typedVirtualizedMatch$1(Typers.scala:4929)
	scala.tools.nsc.typechecker.Typers$Typer.typedOutsidePatternMode$1(Typers.scala:6243)
	scala.tools.nsc.typechecker.Typers$Typer.typed1(Typers.scala:6274)
	scala.tools.nsc.typechecker.Typers$Typer.typed(Typers.scala:6320)
	scala.tools.nsc.typechecker.Typers$Typer.doTypedFunction(Typers.scala:6409)
	scala.tools.nsc.typechecker.Typers$Typer.typedFunction(Typers.scala:3200)
	scala.tools.nsc.typechecker.Typers$Typer.$anonfun$typed1$121(Typers.scala:6202)
	scala.tools.nsc.typechecker.Typers$Typer.typedFunction$1(Typers.scala:512)
	scala.tools.nsc.typechecker.Typers$Typer.typedOutsidePatternMode$1(Typers.scala:6242)
	scala.tools.nsc.typechecker.Typers$Typer.typed1(Typers.scala:6274)
	scala.tools.nsc.typechecker.Typers$Typer.typedVirtualizedMatch$1(Typers.scala:4926)
	scala.tools.nsc.typechecker.Typers$Typer.typedOutsidePatternMode$1(Typers.scala:6243)
	scala.tools.nsc.typechecker.Typers$Typer.typed1(Typers.scala:6274)
	scala.tools.nsc.typechecker.Typers$Typer.typed(Typers.scala:6320)
	scala.tools.nsc.typechecker.Typers$Typer.typedArg(Typers.scala:3562)
	scala.tools.nsc.typechecker.Typers$Typer.handlePolymorphicCall$1(Typers.scala:3966)
	scala.tools.nsc.typechecker.Typers$Typer.doTypedApply(Typers.scala:3985)
	scala.tools.nsc.typechecker.Typers$Typer.normalTypedApply$1(Typers.scala:5261)
	scala.tools.nsc.typechecker.Typers$Typer.typedApply$1(Typers.scala:5272)
	scala.tools.nsc.typechecker.Typers$Typer.typed1(Typers.scala:6264)
	scala.tools.nsc.typechecker.Typers$Typer.typed(Typers.scala:6320)
	scala.tools.nsc.typechecker.Typers$Typer.computeType(Typers.scala:6409)
	scala.tools.nsc.typechecker.Namers$Namer.assignTypeToTree(Namers.scala:1083)
	scala.tools.nsc.typechecker.Namers$Namer.inferredValTpt$1(Namers.scala:1730)
	scala.tools.nsc.typechecker.Namers$Namer.valDefSig(Namers.scala:1743)
	scala.tools.nsc.typechecker.Namers$Namer.memberSig(Namers.scala:1928)
	scala.tools.nsc.typechecker.Namers$Namer.typeSig(Namers.scala:1878)
	scala.tools.nsc.typechecker.Namers$Namer$MonoTypeCompleter.completeImpl(Namers.scala:832)
	scala.tools.nsc.typechecker.Namers$LockingTypeCompleter.complete(Namers.scala:2075)
	scala.tools.nsc.typechecker.Namers$LockingTypeCompleter.complete$(Namers.scala:2073)
	scala.tools.nsc.typechecker.Namers$TypeCompleterBase.complete(Namers.scala:2068)
	scala.reflect.internal.Symbols$Symbol.completeInfo(Symbols.scala:1583)
	scala.reflect.internal.Symbols$Symbol.info(Symbols.scala:1548)
	scala.reflect.internal.Symbols$Symbol.initialize(Symbols.scala:1747)
	scala.tools.nsc.typechecker.Typers$Typer.typed1(Typers.scala:5892)
	scala.tools.nsc.typechecker.Typers$Typer.typed(Typers.scala:6320)
	scala.tools.nsc.typechecker.Typers$Typer.typedStat$1(Typers.scala:6398)
	scala.tools.nsc.typechecker.Typers$Typer.$anonfun$typedStats$10(Typers.scala:3544)
	scala.tools.nsc.typechecker.Typers$Typer.typedStats(Typers.scala:3544)
	scala.tools.nsc.typechecker.Typers$Typer.typedBlock(Typers.scala:2650)
	scala.tools.nsc.typechecker.Typers$Typer.typedOutsidePatternMode$1(Typers.scala:6238)
	scala.tools.nsc.typechecker.Typers$Typer.typed1(Typers.scala:6274)
	scala.tools.nsc.typechecker.Typers$Typer.typed(Typers.scala:6320)
	scala.tools.nsc.typechecker.Typers$Typer.typedDefDef(Typers.scala:6584)
	scala.tools.nsc.typechecker.Typers$Typer.typed1(Typers.scala:6226)
	scala.tools.nsc.typechecker.Typers$Typer.typed(Typers.scala:6320)
	scala.tools.nsc.typechecker.Typers$Typer.typedStat$1(Typers.scala:6398)
	scala.tools.nsc.typechecker.Typers$Typer.$anonfun$typedStats$10(Typers.scala:3544)
	scala.tools.nsc.typechecker.Typers$Typer.typedStats(Typers.scala:3544)
	scala.tools.nsc.typechecker.Typers$Typer.typedTemplate(Typers.scala:2140)
	scala.tools.nsc.typechecker.Typers$Typer.typedModuleDef(Typers.scala:2016)
	scala.tools.nsc.typechecker.Typers$Typer.typed1(Typers.scala:6228)
	scala.tools.nsc.typechecker.Typers$Typer.typed(Typers.scala:6320)
	scala.tools.nsc.typechecker.Typers$Typer.typedStat$1(Typers.scala:6398)
	scala.tools.nsc.typechecker.Typers$Typer.$anonfun$typedStats$10(Typers.scala:3544)
	scala.tools.nsc.typechecker.Typers$Typer.typedStats(Typers.scala:3544)
	scala.tools.nsc.typechecker.Typers$Typer.typedPackageDef$1(Typers.scala:5901)
	scala.tools.nsc.typechecker.Typers$Typer.typed1(Typers.scala:6230)
	scala.tools.nsc.typechecker.Typers$Typer.typed(Typers.scala:6320)
	scala.tools.nsc.typechecker.Analyzer$typerFactory$TyperPhase.apply(Analyzer.scala:125)
	scala.tools.nsc.Global$GlobalPhase.applyPhase(Global.scala:483)
	scala.tools.nsc.interactive.Global$TyperRun.applyPhase(Global.scala:1369)
	scala.tools.nsc.interactive.Global$TyperRun.typeCheck(Global.scala:1362)
	scala.tools.nsc.interactive.Global.typeCheck(Global.scala:680)
	scala.meta.internal.pc.WithCompilationUnit.<init>(WithCompilationUnit.scala:22)
	scala.meta.internal.pc.SimpleCollector.<init>(PcCollector.scala:340)
	scala.meta.internal.pc.PcSemanticTokensProvider$Collector$.<init>(PcSemanticTokensProvider.scala:19)
	scala.meta.internal.pc.PcSemanticTokensProvider.Collector$lzycompute$1(PcSemanticTokensProvider.scala:19)
	scala.meta.internal.pc.PcSemanticTokensProvider.Collector(PcSemanticTokensProvider.scala:19)
	scala.meta.internal.pc.PcSemanticTokensProvider.provide(PcSemanticTokensProvider.scala:73)
	scala.meta.internal.pc.ScalaPresentationCompiler.$anonfun$semanticTokens$1(ScalaPresentationCompiler.scala:195)
```
#### Short summary: 

java.lang.IndexOutOfBoundsException: -1 is out of bounds (min 0, max 2)