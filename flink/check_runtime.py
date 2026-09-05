"""Build both real JVM job graphs without connecting to Kafka or ClickHouse."""

import json
from unittest.mock import patch

import job


def check_graph(env, name):
    """Resolve connector classes, serializers, and checkpoint APIs together."""
    assert name == job.JOB_NAME
    nodes = json.loads(env.get_execution_plan())["nodes"]
    assert nodes and all(node["parallelism"] == 4 for node in nodes)
    keyed_nodes = [node for node in nodes if "count-records-by-tenant" in node["type"]]
    assert bool(keyed_nodes) == job.ENABLE_KEYBY_LAB
    checkpoint = env.get_checkpoint_config()
    assert checkpoint.get_checkpointing_mode() == job.CheckpointingMode.EXACTLY_ONCE
    assert checkpoint.get_checkpoint_interval() == 10_000
    assert checkpoint.get_externalized_checkpoint_retention() == (
        job.ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION
    )


def main():
    # Calling get_execution_plan performs Java graph construction; execute is
    # replaced only at the submission boundary, so no external services run.
    with patch.object(job.StreamExecutionEnvironment, "execute", check_graph):
        for enabled in (False, True):
            with patch.object(job, "ENABLE_KEYBY_LAB", enabled):
                job.main()
    print("Both Flink job graphs and checkpoint policies are compatible.")


if __name__ == "__main__":
    main()
