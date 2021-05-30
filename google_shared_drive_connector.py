import datetime
import io
import time
from dataclasses import dataclass
from typing import List

import google.auth
import pandas as pd
import unidecode
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from send_mail import SendMail
from google_shared_drive_configs import configs

# BQ does not allow these chars in table names
chars_to_replace = [' ', '%', '-', '(', ')', '[', ']', '/']

bq_types_mapping = {
    int: 'INT64',
    str: 'STRING',
    float: 'FLOAT64',
    bool: 'BOOLEAN'
}
folder3_columns_to_float = ['column_4', 'column_5']

load_dotenv()


@dataclass
class DriveConnector:
    '''Connector moves data from Google Shared Drive to Big Query.'''
    shared_drive_name: str
    dataset: str
    recipients: List
    service = None
    files_added = []
    counter = 1

    def __post_init__(self):
        '''Set credentials, service to call Drive API, BQ Client and Mail.'''

        credentials, self.project = self.get_credentials()
        self.service = build('drive', 'v3', credentials=credentials)
        self.bq_client =\
            bigquery.Client(credentials=credentials, project=self.project)
        self.mail = SendMail(recipients=self.recipients)

    def get_credentials(self):
        '''Based on GOOGLE_APPLICATION_CREDENTIALS get Google credentials.'''

        credentials, project = google.auth.default(
            scopes=[
                'https://www.googleapis.com/auth/drive',
                'https://www.googleapis.com/auth/bigquery',
            ]
        )
        return credentials, project

    def get_shared_drive_id(self):
        '''Get Shared Drive ID by calling the Drive v3 API.'''

        results = self.service.drives().list(pageSize=10).execute()
        items = results.get('drives', [])

        if not items:
            RuntimeError('No drives found.')
        else:
            for item in items:
                if item['name'] == self.shared_drive_name:
                    shared_drive_id = item['id']
                    return shared_drive_id
                else:
                    RuntimeError('Shared Drive not found.')

    def create_search_file_query(self):
        '''Create search query for files to be found.'''

        # search-files query
        # https://developers.google.com/drive/api/v3/search-files
        hour = '00:00:00'
        # Search only files created or updated since yesterday
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        yesterday = yesterday.strftime(f'%Y-%m-%dT{hour}')
        return (
            f"modifiedTime > '{yesterday}' "
            # "and ('folder_id' in parents) "
            "and (mimeType = 'text/csv')")

    def get_items_ids(self, shared_drive_id):
        '''Get item's IDs on Drive by calling the Drive v3 API.'''

        query = self.create_search_file_query()
        fields = 'nextPageToken, files(id, name, mimeType, parents, trashed)'
        results = self.get_files_from_drive(shared_drive_id, query, fields)
        return results

    def get_files_from_drive(self, shared_drive_id, query, fields):
        '''Execute query on Drive and get results with files.'''

        # Getting File from Drive
        results = self.service.files().list(
            q=query,
            pageSize=1000,
            # files reference
            # https://developers.google.com/drive/api/v3/reference/files
            fields=fields,
            driveId=shared_drive_id,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            spaces='drive',
            corpora='drive').execute()
        return results.get('files', [])

    def get_folders_ids(self, shared_drive_id):
        '''Execute query on Drive and get results with folders IDs.'''

        query = "mimeType = 'application/vnd.google-apps.folder'"
        fields = 'nextPageToken, files(id, name)'
        results = self.get_files_from_drive(shared_drive_id, query, fields)
        return results

    def change_str_with_comma_to_float(self, df, column):
        '''Change column with commas in Pandas df to float.'''

        to_replace = {
            ',': '.',
            ' ': '',
            '\xa0': ''
        }
        df = df.replace({column: to_replace}, regex=False)
        df[column] = df[column].astype(float)
        return df

    def prepare_df_folder3(self, df):
        '''Prepare df for folder3.'''

        # there are couple of columns with float values but with commas
        # instead of dots, or with white spaces - cleaning needed
        for column in folder3_columns_to_float:
            df = self.change_str_with_comma_to_float(df, column)
        return df

    def prepare_df_folder12(self, df):
        '''Prepare df for folder1 and folder2.'''

        if '[-] ' in df.columns:
            df = df.rename(columns={'[-] ': '[-]'})
        if '[+] ' in df.columns:
            df = df.rename(columns={'[+] ': '[+]'})
        return df

    def prepare_df(self, df, file_name, folder_name):
        '''Prepare df based on folder_name, add ts_ms and clean col names.'''

        if folder_name in ('folder_3',):
            df = self.prepare_df_folder3(df)
        if folder_name in ('folder_1', 'folder_2'):
            df = self.prepare_df_folder12(df)
        df['file'] = file_name
        current_ts_ms = int(round(time.time() * 1000))
        df.insert(0, 'ts_ms', pd.to_datetime(current_ts_ms, unit='ms'))
        df.columns = self.clean_column_names(df.columns)
        return df

    def create_schema(self, folder_name):
        '''Create schema based on types from config file.'''

        # create schema based on types from config file (used to load df)
        schema = []
        dtypes = configs[folder_name]['dtypes']
        for column, type in dtypes.items():
            if (folder_name == 'folder_3'
                    and column in folder3_columns_to_float):
                type = float
            column_cleaned = self.clean_column_str(column)
            schema.append(
                {
                    'name': column_cleaned,
                    'type': bq_types_mapping[type],
                    'description': column
                }
            )
        date_cols = configs[folder_name].get('date_cols')
        if date_cols:
            for date_col in date_cols:
                date_col_clean = self.clean_column_str(date_col)
                schema.append(
                    {
                        'name': date_col_clean,
                        'type': 'TIMESTAMP',
                        'description': date_col
                    }
                )
        return schema

    def upload_df_to_bq(self, df, folder_name, file_name):
        '''Upload df to Big Query by executing job.'''

        # https://cloud.google.com/bigquery/quotas#standard_tables
        # be nice to API, do not send too many requests
        if self.counter % 5 == 0:
            time.sleep(10)
        table_id = f'{self.dataset}.{folder_name}'
        schema = self.create_schema(folder_name)
        job_config = bigquery.LoadJobConfig(
            write_disposition='WRITE_APPEND',
            schema=schema
        )
        try:
            job = self.bq_client.load_table_from_dataframe(
                df,
                table_id,
                job_config=job_config,
            )
            job.result()  # Wait for the job to complete.
            print(f'table {table_id} uploaded to BQ.')
        except ValueError as error:
            self.send_error_mail(file_name, folder_name, error)
        self.counter += 1

    def clean_column_str(self, column):
        '''Replace unwanted values in column name.'''

        column = column.replace('[+]', 'plus')
        column = column.replace('[-]', 'minus')
        for char in chars_to_replace:
            column = column.replace(char, '_')
        return unidecode.unidecode(column)

    def clean_column_names(self, columns):
        '''Replace unwanted values in column names.'''

        columns = columns.str.replace('[+]', 'plus', regex=False)
        columns = columns.str.replace('[-]', 'minus', regex=False)
        for char in chars_to_replace:
            columns = columns.str.replace(char, '_', regex=False)
        columns = [unidecode.unidecode(column) for column in columns]
        return columns

    def get_df_from_file_id(self, file_id, file_name, folder_name):
        '''Download df from Drive based on file ID.'''

        config = configs[folder_name]
        params = {
            'encoding': config.get('encoding'),
            'skiprows': config.get('skip_rows'),
            'parse_dates': config.get('date_cols'),
            'dayfirst': config.get('dayfirst'),
            'dtype': config.get('dtypes'),
            'delimiter': config.get('delimiter', ','),
            'decimal': config.get('decimal', '.'),
        }
        request = self.service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(f'Download {file_name} {int(status.progress() * 100)}%.')
        fh.seek(0)
        df = pd.read_csv(fh, **params)
        return df

    def send_error_mail(self, file_name, folder_name, error):
        '''Send mail with error text.'''

        mail_subject = 'Drive to BQ Upload: Error while loading file'
        error_text = (
            f'Error while loading {file_name} from {folder_name}\n'
            f'Error: {error}')
        print(error_text)
        message = self.mail.create_message(mail_subject, error_text)
        self.mail.send_mail(message)

    def upload_csv_to_bq(self, folder_name, file_name, file_id):
        '''Download CSV as df based on file_id and upload it to Big Query.'''

        print(f'Uploading, folder: {folder_name}, file: {file_name}')
        try:
            df = self.get_df_from_file_id(file_id, file_name, folder_name)
            if df.empty:
                raise ValueError('CSV is empty!')
        except ValueError as error:
            self.send_error_mail(file_name, folder_name, error)
            return
        df = self.prepare_df(df, file_name, folder_name)
        # print(dict(zip(df.columns, df.iloc[0])))
        self.upload_df_to_bq(df, folder_name, file_name)
        self.files_added.append(file_name)

    def if_table_not_in_bq(self, folder_name, file_name):
        '''Check if table not in Big Query already.'''

        sql_query = (f'''
        SELECT `file`
        FROM `{self.project}.{self.dataset}.{folder_name}`
        WHERE `file` = '{file_name}'
        LIMIT 1;
        ''')
        query_job = self.bq_client.query(sql_query)
        try:
            result = query_job.result()
            if result.total_rows != 0:
                # This file is already in BQ!
                return False
        except NotFound:
            # Table not exists
            return True
        # Not in BQ, proceed
        return True

    def iterate_through_items(self, items, folders_dict):
        '''Iterate through items on Drive and upload matched to Big Query.'''

        for item in items:
            if item['trashed']:
                continue
            parent = item['parents'][0]
            if folders_dict.get(parent):
                file_id = item['id']
                file_name = item['name']
                folder_name = folders_dict[parent]
                if folder_name in configs['all_folders']:
                    folder_name = folder_name.replace(' ', '_')
                    if self.if_table_not_in_bq(folder_name, file_name):
                        self.upload_csv_to_bq(
                            folder_name, file_name, file_id)
        self.send_confirmation_mail()

    def send_confirmation_mail(self):
        '''Send an email with confirmation which tables were uploaded.'''

        if self.files_added:
            files_added_txt = "\n".join(self.files_added)
            mail_text = 'Files added: \n' + files_added_txt
            mail_subject = 'Drive to BQ Upload: Transfer completed'
        else:
            mail_text = 'No file from Drive was added to BQ'
            mail_subject = 'Drive to BQ Upload: No file uploaded'
        message = self.mail.create_message(mail_subject, mail_text)
        self.mail.send_mail(message)

    def run_transfer(self):
        '''Run the Drive to Big Query connector.'''

        shared_drive_id = self.get_shared_drive_id()
        folders_ids = self.get_folders_ids(shared_drive_id)
        folders_dict = {d['id']: d['name'] for d in folders_ids}
        items = self.get_items_ids(shared_drive_id)
        if not items:
            RuntimeError('No files found on shared drive. Process Completed.')
        else:
            self.iterate_through_items(items, folders_dict)


if __name__ == "__main__":
    bq_dataset = 'your_bq_dataset'
    shared_drive_name = 'shared_drive_name'
    recipients = ['example@gmail.com']

    data = {
        'dataset': bq_dataset,
        'shared_drive_name': shared_drive_name,
        'recipients': recipients
    }

    sgd = DriveConnector(**data)
    sgd.run_transfer()
