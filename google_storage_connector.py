import gzip
from dataclasses import dataclass
from typing import List

import gcsfs
from dotenv import load_dotenv
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery, storage


@dataclass
class StorageConnector:
    '''Connector moves data from Google Cloud Storage to Big Query.'''
    bucket_name: str
    project_name: str
    dataset: str
    tables_to_skip: List

    def __post_init__(self):
        '''Handle connections to BQ and storage.'''
        load_dotenv()  # Load path to GOOGLE_APPLICATION_CREDENTIALS json
        storage_client = storage.Client()
        self.bq_client = bigquery.Client()
        self.bucket = storage_client.get_bucket(self.bucket_name)
        self.storage_file_system =\
            gcsfs.GCSFileSystem(project=self.project_name)

    def create_job_config(self):
        '''Create BQ load job config to properly ingest the data.'''
        # schema = [
        #     {
        #         "description": "id_event",
        #         "mode": "REQUIRED",
        #         "name": "id_event",
        #         "type": "INT64"
        #     }, ]
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            # schema autodetected, or you can define it:
            # schema=schema,
            skip_leading_rows=1,
            allow_quoted_newlines=True,
            # The source format defaults to CSV, so the line below is optional.
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter=";"
        )
        return job_config

    def get_uri_from_blob(self, blob):
        '''Create URI from blob object.'''
        uri = f"gs://{self.bucket_name}/{blob.name}"
        return uri

    def get_table_name_from_blob(self, blob):
        '''Get table name from blob object.'''
        table_name = blob.name.split("/")[1].split(".")[0]
        return table_name

    def upload_data_to_bq(self, blob_uri, table_name):
        '''Load file to BQ based on Storage Blob URI.'''
        table_id = f"{self.project_name}.{self.dataset}.{table_name}"
        job_config = self.create_job_config()
        load_job = self.bq_client.load_table_from_uri(
            blob_uri, table_id, job_config=job_config
        )  # Make an API request.
        load_job.result()  # Waits for the job to complete.
        destination_table = self.bq_client.get_table(table_id)
        return destination_table.num_rows

    def check_if_table_to_skip(self, table_name):
        '''Check if table should be skipped.'''
        for t_to_skip in self.tables_to_skip:
            if t_to_skip in table_name:
                return True
        return False

    def count_lines(self, blob):
        '''Count lines in file on GC Storage.'''
        gcs_file = f'{self.bucket_name}/{blob.name}'
        with self.storage_file_system.open(gcs_file, 'rb') as f:
            with gzip.open(f, 'rb') as g:
                for lines, l in enumerate(g):
                    pass
            print("Rows counted: ", lines)
            return lines

    def check_if_all_rows_uploaded(self, blob, blob_uri, table_name):
        '''Check if no of rows on Storage the same as uploaded to BQ.'''
        rows_loaded = self.upload_data_to_bq(blob_uri, table_name)
        lines = self.count_lines(blob)
        if rows_loaded == lines:
            print(f"Loaded {rows_loaded} rows.")
        else:
            print(f"Different number of rows!! Loaded: {rows_loaded}"
                  f"vs from csv: {lines}")

    def load_data_from_storage(self, years):
        '''Load all matching files from Storage to Big Query.'''
        # Load only files with proper prefix name: year
        for year in years:
            for blob in self.bucket.list_blobs(prefix=year):
                blob_uri = self.get_uri_from_blob(blob)
                table_name = self.get_table_name_from_blob(blob)
                to_skip = self.check_if_table_to_skip(table_name)
                if to_skip:
                    print(f"{table_name} skipped")
                    continue
                try:
                    self.check_if_all_rows_uploaded(blob, blob_uri, table_name)
                except BadRequest as e:
                    print(f"{table_name} skipped, because of error: {e}")


if __name__ == "__main__":
    data = {
        'bucket_name': 'your_bucket',
        'project_name': 'your_project',
        'dataset': 'your_dataset',
        'tables_to_skip': []
    }

    sc = StorageConnector(**data)
    years = ("2018", "2019", "2020")
    # LOAD FROM STORAGE ONLY FOR GIVEN YEARS
    sc.load_data_from_storage(years)
