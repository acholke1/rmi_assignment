"""Batch Dataflow pipeline: read preprocessed data from BigQuery, group by job+month, call ML model, write predictions to BigQuery.

Usage (local):
python -m dataflow.df_ml_salary_prediction_pipeline --runner=DirectRunner --input_query="SELECT * FROM dataset.preprocessed_job_posting LIMIT 100" --output_table=your-project:dataset.predicted_salary_per_month
"""
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import json

# import model function (mock)
from dataflow.predict_salary_model import predict_salary_of_job

def row_to_keyed(element):
    # element is a dict (from BigQuery)
    job = element.get('title') or element.get('job_title')
    month = element.get('year_month') or element.get('posted_month')
    key = f"{job}||{month}"
    return (key, element)

def aggregate_and_predict(key_elements):
    key, elements = key_elements
    # convert to list (pandas could be used but avoid heavy deps in the pipeline transforms)
    import pandas as pd
    df = pd.DataFrame(list(elements))
    # call the provided model function, expect a float or dict
    pred = predict_salary_of_job(df)
    job, month = key.split('||')
    return {
        'title': job,
        'year_month': month,
        'predicted_salary': float(pred)
    }

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_query', required=True, help='BigQuery SQL to read preprocessed data')
    parser.add_argument('--output_table', required=True, help='BigQuery table spec PROJECT:DATASET.TABLE')
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as p:
        rows = p | 'ReadFromBQ' >> beam.io.ReadFromBigQuery(query=known_args.input_query, use_standard_sql=True, flatten_results=True)
        preds = (rows
                 | 'KeyByJobMonth' >> beam.Map(row_to_keyed)
                 | 'Group' >> beam.GroupByKey()
                 | 'Predict' >> beam.Map(aggregate_and_predict))

        schema = {
            'fields': [
                {'name':'title','type':'STRING','mode':'NULLABLE'},
                {'name':'year_month','type':'STRING','mode':'NULLABLE'},
                {'name':'predicted_salary','type':'FLOAT','mode':'NULLABLE'}
            ]
        }

        preds | 'WriteToBQ' >> beam.io.WriteToBigQuery(
            known_args.output_table,
            schema=schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

if __name__ == '__main__':
    run()
