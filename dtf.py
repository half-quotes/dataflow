import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText, WriteToBigQuery
import logging
import os


# Define your source and target tables
source_table = 'half-poc:dtf.table1'
target_table = 'half-poc:dtf.table2_curr'

# Define your schema (replace with actual schema)
schema = 'Full_Name:STRING, Age:INTEGER, Gender:STRING'

def merge_names(element):
    # Merge first and last names into a new 'full_name' column
    element['Full_Name'] = f"{element['First_Name']} {element['Last_Name']}"
    del element['First_Name']  # Remove the 'first_name' column
    del element['Last_Name']  # Remove the 'last_name' column
    return element

# Create a pipeline
def run_pipeline(argv=None):
    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        data = (
            p
            | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(table=source_table))
            | 'Merge Names' >> beam.Map(merge_names)  # Add the new 'full_name' column
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                table=target_table,
                schema=schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
        )
        
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()

