import argparse
import json
import logging
import time

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.external import JavaJarExpansionService


# 1. The Schema Definition
# We define what the data looks like for the Snowflake Streaming API
# This maps to the columns in our Snowflake table.
def get_snowflake_schema():
    return {
        "fields": [
            {"name": "record_content", "type": "VARIANT"},
            {"name": "metadata", "type": "VARIANT"},
            {"name": "ingested_at", "type": "TIMESTAMP_LTZ"}
        ]
    }


# 2. The Transformation Logic
class StandardizeMessage(beam.DoFn):
    """
    Parses the raw Pub/Sub message, ensures valid JSON,
    and decorates it with processing metadata.
    """

    def process(self, element):
        try:
            # Pub/Sub messages come as bytes
            raw_content = json.loads(element.decode("utf-8"))

            # Construct the row for Snowflake
            yield {
                "record_content": raw_content,
                "metadata": {
                    "source": "pubsub",
                    "processed_by": "dataflow-hyperion-v1",
                    "latency_check": time.time()
                },
                "ingested_at": time.time()  # mapped to TIMESTAMP_LTZ
            }
        except Exception as e:
            # In a real scenario, we would tag this as a 'bad record'
            # and send it to a side-output (Dead Letter)
            logging.error(f"Failed to parse record: {e}")


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_subscription', required=True)
    parser.add_argument('--snowflake_server', required=True)
    parser.add_argument('--snowflake_table', required=True)

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args, streaming=True)

    with beam.Pipeline(options=pipeline_options) as p:
        # A. Read from the Pub/Sub Subscription created in Phase 1
        messages = (
                p
                | "ReadFromPubSub" >> beam.io.ReadFromPubSub(
            subscription=known_args.input_subscription
        )
        )

        # B. Transform: Parse and Format
        rows = (
                messages
                | "ParseAndFormat" >> beam.ParDo(StandardizeMessage())
        )

        # C. Write: Cross-Language Transform to Snowflake Streaming
        # This uses the Java SDK's SnowflakeIO under the hood for true streaming
        rows | "WriteToSnowflakeStreaming" >> beam.io.WriteToSnowflake(
            server_name=known_args.snowflake_server,
            warehouse="COMPUTE_WH",
            database="HYPERION_DB",
            schema="PUBLIC",
            table=known_args.snowflake_table,
            storage_integration_name="GCP_INTEGRATION",  # Required for auth handshakes

            # THE KEY SENIOR CONFIGURATION:
            # We explicitly invoke the external Java transform expansion
            expansion_service=JavaJarExpansionService('path/to/snowflake-beam-connector.jar'),

            # Configuration map to enable Streaming API (conceptually)
            # Note: Actual params depend on the specific version of the X-lang wrapper
            create_disposition='CREATE_NEVER',
            write_disposition='WRITE_APPEND'
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
