import apache_beam as beam
from apache_beam.io import ReadFromPubSub
from apache_beam.io import WriteToPubSub
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from fastavro import parse_schema, schemaless_reader, schemaless_writer
import argparse
import io
import datetime
import logging
from apache_beam.transforms.trigger import *
from apache_beam.transforms.userstate import *
from apache_beam.coders import *
import time
import utils

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(name)s : %(message)s", level=logging.INFO
)
logging = logging.getLogger(__name__)

raw_schema = {
    "type": "record",
    "namespace": "AvroPubSubDemo",
    "name": "Entity",
    "fields": [
            {"name": "id", "type": "string"},
            {"name": "type", "type": "string"},
            {"name": "gh6", "type": "string"},
            {"name": "score", "type": "int"},
            {"name": "ts", "type": "int"},
    ],
}

model_to_schema = {
    "Model 1": {"Mobile10"},
    "Model 2": {"Mobile10", "Browser30", "Browser30"},
    "Model 3": {"Mobile10", "Request30"}
}


class avroReadWrite:
    def __init__(self, schema):
        self.schema = schema

    def deserialize(self, record):
        bytes_reader = io.BytesIO(record)
        dict_record = schemaless_reader(bytes_reader, self.schema)
        return dict_record

    def serialize(self, record):
        bytes_writer = io.BytesIO()
        schemaless_writer(bytes_writer, self.schema, record)
        bytes_array = bytes_writer.getvalue()
        return bytes_array


class JobOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--input", required=True,
                            help="pubsub input topic")
        parser.add_argument("--output", required=True,
                            help="pubsub outut topic")


class TransformerDoFn(beam.DoFn):
    def __init__(self, _schema, root):
        self.schema = _schema

    def process(self, record):
        timestampedVal = beam.window.TimestampedValue(
            record, time.time() + record['ts'])

        yield timestampedVal


class RunModel(beam.DoFn):
    BUFFER_STATE = BagStateSpec('inputs_seen', StrUtf8Coder())

    def process(self, element, inputs_seen=beam.DoFn.StateParam(BUFFER_STATE)):
        features = element[1]

        for feature in features:
            for k in feature.keys():
                inputs_seen.add(k)

        seen_features = {x for x in inputs_seen.read()}

        for k, v in model_to_schema.items():
            if v.issubset(seen_features):
                print("Triggered: " + k + " for: " + element[0])


def print_windows(element, window=beam.DoFn.WindowParam,  pane_info=beam.DoFn.PaneInfoParam, timestamp=beam.DoFn.TimestampParam):
    print(window)
    print(pane_info)
    print(timestamp)
    print(element)
    print('-----------------')


def process_with_side_input(element, side):
    key = element[0]

    for k, v in element[1].items():
        if k == 'tag':
            continue

        yield (key, {k + element[1]['tag']: v})

    for k, v in side[key].items():
        if k == 'tag':
            continue

        yield (key, {k + side[key]['tag']: v})


def emit_writes(element):
    features = list(element[1].keys())

    return [(element[0], f) for f in features]


def emit_writes2(element):
    features = list(element[1].keys())

    return [(element[0], "30 sec: " + f) for f in features]


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session
    job_options = pipeline_options.view_as(JobOptions)
    start = time.time()

    with beam.Pipeline(options=pipeline_options) as p:
        ten_second_combine_fn = utils.get_CalculateFeaturesPerFeatureCombineFn(
            '10')
        thirty_second_combine_fn = utils.get_CalculateFeaturesPerFeatureCombineFn(
            '30')

        schema = parse_schema(raw_schema)
        avroRW = avroReadWrite(schema)
        source = ReadFromPubSub(subscription=str(job_options.input))
        sink = WriteToPubSub(str(job_options.output))

        lines = (
            p
            | "read" >> source
            | "deserialize" >> beam.Map(lambda x: avroRW.deserialize(x))
            | "process" >> (beam.ParDo(TransformerDoFn(_schema=schema, root=start)))
            | "key" >> beam.Map(lambda e: (e['gh6'], e))
        )

        fixed_windows = (
            lines
            | "10 second window" >> beam.WindowInto(
                beam.window.FixedWindows(10),
                trigger=AfterWatermark(),
                accumulation_mode=beam.transforms.trigger.AccumulationMode.DISCARDING
            )
            | "Combine 10 second fixed windows" >> beam.CombinePerKey(ten_second_combine_fn)
        )

        windows = (
            lines
            | beam.WindowInto(
                beam.window.SlidingWindows(30, 10),
                trigger=AfterWatermark(),
                accumulation_mode=beam.transforms.trigger.AccumulationMode.DISCARDING
            )
            | "Combine 30 second windows" >> beam.CombinePerKey(thirty_second_combine_fn)
        )

        run_models = (
            fixed_windows
            | beam.ParDo(process_with_side_input, side=beam.pvalue.AsDict(windows))
            | beam.GroupByKey()
            | beam.ParDo(RunModel())
        )

    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    run()
