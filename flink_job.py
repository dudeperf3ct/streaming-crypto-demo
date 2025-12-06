import os

from pyflink.common import Duration, Row, Time, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.window import TumblingEventTimeWindows

TOPIC = "trades.BTCUSDT"
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Kafka Source (event-time semantics)
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(TOPIC)
        .set_group_id("flink-stream")
        .set_value_only_deserializer(
            JsonRowDeserializationSchema.builder()
            .type_info(
                Types.ROW_NAMED(
                    ["symbol", "price", "quantity", "ts"],
                    [Types.STRING(), Types.FLOAT(), Types.FLOAT(), Types.LONG()],
                )
            )
            .build()
        )
        .build()
    )

    stream = env.from_source(
        source,
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(2)  # create a window of events
        ).with_timestamp_assigner(
            lambda event, _: event[3]  # event timestamp = ts
        ),
        "Kafka Source",
    )

    # Key by symbol and apply window
    windowed = (
        stream.key_by(lambda e: e[0])
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .reduce(
            lambda a, b: Row(
                a[0],  # symbol
                a[1] + b[1],  # sum price (or use quantity if that’s what you want)
                0.0,  # placeholder quantity/avg
                max(a[3], b[3]),  # ts
            )
        )
    )

    windowed.print()

    env.execute("BTCUSDT Streaming Average")


if __name__ == "__main__":
    main()
