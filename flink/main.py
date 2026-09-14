import json
import os

from pyflink.common import SimpleStringSchema, Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "recent-change")
WIKI_DOMAIN = os.environ.get("WIKI_DOMAIN", "en.wikipedia.org")

# TODO figure out what these should be
BURST_WINDOW_MS = 10 * 60 * 1000
BURST_THRESHOLD = 2


def parse_event(raw: str):
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def is_real_edit(event) -> bool:
    if event is None:
        return False
    return (
        event.get("type") == "edit"
        and event.get("namespace") == 0
        and not event.get("bot", False)
        and event.get("meta", {}).get("domain") == WIKI_DOMAIN
    )


class BurstDetector(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        self.count_state = runtime_context.get_state(
            ValueStateDescriptor("edit_count", Types.LONG())
        )
        self.window_start_state = runtime_context.get_state(
            ValueStateDescriptor("window_start", Types.LONG())
        )

    def process_element(self, event, ctx: "KeyedProcessFunction.Context"):
        now = ctx.timer_service().current_processing_time()
        window_start = self.window_start_state.value()

        if window_start is None or now - window_start > BURST_WINDOW_MS:
            window_start = now
            self.window_start_state.update(window_start)
            self.count_state.update(0)

        count = (self.count_state.value() or 0) + 1
        self.count_state.update(count)

        if count == BURST_THRESHOLD:
            minutes = BURST_WINDOW_MS // 60000
            yield f"BURST: '{ctx.get_current_key()}' had {count} edits in {minutes} min"


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

    parsed = stream.map(parse_event, output_type=Types.PICKLED_BYTE_ARRAY())
    edits = parsed.filter(is_real_edit)
    keyed = edits.key_by(lambda event: event["title"])
    alerts = keyed.process(BurstDetector(), output_type=Types.STRING())

    alerts.print()

    env.execute("wiki-streamer burst detector")


if __name__ == "__main__":
    main()
