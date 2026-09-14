FROM flink:1.20-scala_2.12-java17

# The taskmanager executes Python UDFs and the jobmanager runs `flink run -py`,
# so both need Python + the real PyFlink package (pinned to match the cluster's
# Flink version). Note: the PyPI package is "apache-flink", NOT "pyflink" -
# "pyflink" is an unrelated, empty placeholder package.
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --no-cache-dir apache-flink==1.20.5 && \
    rm -rf /var/lib/apt/lists/*

# Kafka connector jar (DataStream KafkaSource/KafkaSink), version pinned to match
# the 1.20 cluster.
RUN curl -fSL -o /opt/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar
