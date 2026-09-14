import json
import os

from pyflink.common import SimpleStringSchema, Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "recent-change")


def describe_event(raw: str) -> str:
    try:
        event = json.loads(raw)
    except json.JSONDecodeError:
        return f"unparsed event: {raw[:100]}"
    return f"{event.get('type', '?')} | {event.get('title', '?')}"


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(KAFKA_TOPIC)
        .set_group_id("wiki-streamer-flink")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        source, WatermarkStrategy.no_watermarks(), "recent-change-source"
    )

    stream.map(describe_event, output_type=Types.STRING()).print()

    env.execute("wiki-streamer hello world")


if __name__ == "__main__":
    main()
