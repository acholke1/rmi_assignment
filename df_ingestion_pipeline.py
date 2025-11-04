"""Dataflow streaming pipeline: Pub/Sub -> window -> parse -> BigQuery

Run locally for testing:
python -m dataflow.df_ingestion_pipeline --runner=DirectRunner --input_file=sample_data/raw_job_postings.json

When running on Dataflow set runner=DataflowRunner and supply project, region, temp_location, staging_location.
"""
import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions

class ParseJobPostingDoFn(beam.DoFn):
    def process(self, element):
        # element: bytes or string (Pub/Sub payload)
        if isinstance(element, bytes):
            element = element.decode('utf-8')
        try:
            record = json.loads(element)
        except Exception:
            # skip malformed
            return
        # Add ingestion_time
        import datetime
        record.setdefault('ingestion_time', datetime.datetime.utcnow().isoformat() + 'Z')
        # enforce fields & types (basic)
        output = {
            'job_id': str(record.get('job_id')),
            'title': record.get('title'),
            'posted_date': record.get('posted_date'),
            'min_salary': float(record.get('min_salary')) if record.get('min_salary') is not None else None,
            'max_salary': float(record.get('max_salary')) if record.get('max_salary') is not None else None,
            'company': record.get('company'),
            'location': record.get('location'),
            'description': record.get('description'),
            'ingestion_time': record.get('ingestion_time')
        }
        yield output

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_topic', help='Pub/Sub topic path', default=None)
    parser.add_argument('--input_file', help='Local newline-delimited JSON file for testing', default=None)
    parser.add_argument('--output_table', help='BigQuery table spec <PROJECT:DATASET.TABLE>', required=False, default='your-project:dataset.raw_job_posting')
    parser.add_argument('--window_size', type=int, default=60)
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).streaming = True if known_args.input_topic else False

    with beam.Pipeline(options=options) as p:
        if known_args.input_topic:
            messages = p | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic=known_args.input_topic).with_output_types(bytes)
            parsed = (messages
                      | 'Window' >> beam.WindowInto(beam.window.FixedWindows(known_args.window_size))
                      | 'Decode' >> beam.Map(lambda b: b.decode('utf-8'))
                      | 'Parse' >> beam.ParDo(ParseJobPostingDoFn()))
        else:
            # local file mode for testing
            lines = p | 'ReadFile' >> beam.io.ReadFromText(known_args.input_file)
            parsed = lines | 'Parse' >> beam.ParDo(ParseJobPostingDoFn())

        table_schema = {
            'fields': [
                {'name':'job_id','type':'STRING','mode':'REQUIRED'},
                {'name':'title','type':'STRING','mode':'NULLABLE'},
                {'name':'posted_date','type':'DATE','mode':'NULLABLE'},
                {'name':'min_salary','type':'FLOAT','mode':'NULLABLE'},
                {'name':'max_salary','type':'FLOAT','mode':'NULLABLE'},
                {'name':'company','type':'STRING','mode':'NULLABLE'},
                {'name':'location','type':'STRING','mode':'NULLABLE'},
                {'name':'description','type':'STRING','mode':'NULLABLE'},
                {'name':'ingestion_time','type':'TIMESTAMP','mode':'NULLABLE'}
            ]
        }

        (parsed
         | 'WriteToBQ' >> beam.io.WriteToBigQuery(
             known_args.output_table,
             schema=table_schema,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
         ))

if __name__ == '__main__':
    run()
