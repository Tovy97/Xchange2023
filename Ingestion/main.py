import logging
from decimal import Decimal
from io import BytesIO
from typing import *

import functions_framework
import pandas
import pandas_gbq
from cloudevents.http.event import CloudEvent
from google.cloud.logging import Client as GoogleCloudLoggingClient
from google.cloud.secretmanager import SecretManagerServiceClient as GoogleCloudSecretManagerClient
from google.cloud.storage import Client as GoogleCloudStorageClient
from google_crc32c import Checksum
from pandas import DataFrame
from pyzipper import AESZipFile

logging_client: GoogleCloudLoggingClient = GoogleCloudLoggingClient()
google_cloud_storage_client: GoogleCloudStorageClient = GoogleCloudStorageClient()

logging_client.setup_logging()

PROJECT_ID: str = 'goreply-xchange2023-datastudio'
SECRET_ID: str = 'csv_file_decryption_password'
ARCHIVE_BUCKET: str = 'xchange-23_archive'

FILE_TABLE_MAPPING: Dict[str, str] = {
    'orders.csv': 'Xchange_23.Orders',
    'rows_of_orders.csv': 'Xchange_23.OrdersRows'
}
TABLE_SCHEMA: Dict[str, List[Dict[str, str]]] = {
    'orders.csv': [
        {'name': 'order_id', 'type': 'STRING(50) REQUIRED'},
        {'name': 'customer_name', 'type': 'STRING REQUIRED'},
        {'name': 'price', 'type': 'NUMERIC(7,2) REQUIRED'},
        {'name': 'currency', 'type': 'STRING(3) REQUIRED'},
        {'name': 'order_date', 'type': 'DATE REQUIRED'},
        {'name': 'city', 'type': 'STRING REQUIRED'},
        {'name': 'country', 'type': 'STRING(2) REQUIRED'}
    ],
    'rows_of_orders.csv': [
        {'name': 'order_id', 'type': 'STRING(50) REQUIRED'},
        {'name': 'product_name', 'type': 'STRING REQUIRED'},
        {'name': 'price_per_unit', 'type': 'NUMERIC(7,2) REQUIRED'},
        {'name': 'quantity', 'type': 'INTEGER REQUIRED'},
        {'name': 'total_price', 'type': 'NUMERIC(7,2) REQUIRED'}
    ]
}
FILE_COLUMN_TYPE: Dict[str, Dict[str, Dict[str, Type]]] = {
    'orders.csv': {
        'dtype': {
            'order_id': str,
            'customer_name': str,
            'currency': str,
            'order_date': str,
            'city': str,
            'country': str
        },
        'converters': {
            'price': Decimal
        },
    },
    'rows_of_orders.csv': {
        'dtype': {
            'order_id': str,
            'product_name': str,
            'quantity': int
        },
        'converters': {
            'price_per_unit': Decimal,
            'total_price': Decimal
        }
    }
}


def read_password_from_secret_manager(secret_id: str, version_id: str = 'latest') -> bytes:
    secret_manager_client = GoogleCloudSecretManagerClient()
    secret = secret_manager_client.access_secret_version(
        name=f'projects/{PROJECT_ID}/secrets/{secret_id}/versions/{version_id}'
    )
    crc32c = Checksum()
    crc32c.update(secret.payload.data)
    if secret.payload.data_crc32c != int(crc32c.hexdigest(), 16):
        raise Exception('Data corruption detected in payload read from secret manager')
    return secret.payload.data


def read_zip_file_from_gcs(bucket_name: str, filename: str) -> BytesIO:
    bucket = google_cloud_storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)
    return BytesIO(blob.download_as_bytes())


def read_zip_file_from_disk(filename: str) -> BytesIO:
    # For debug purposes only, path to be adjusted
    with open(f'../Data Generator/output/{filename}', 'rb') as file:
        return BytesIO(file.read())


def unzip_files(zipped_file: BytesIO) -> Dict[str, DataFrame]:
    files = {}
    with AESZipFile(zipped_file, mode="r") as archive:
        password = read_password_from_secret_manager(SECRET_ID)
        archive.setpassword(password)
        for file in archive.filelist:
            filename = file.filename
            files[filename] = pandas.read_csv(
                BytesIO(archive.read(file)),
                dtype=FILE_COLUMN_TYPE[filename]['dtype'],
                converters=FILE_COLUMN_TYPE[filename]['converters'],
                na_filter=False
            )
    return files


def load_file_on_big_query(filename: str, file: DataFrame) -> None:
    table_id = FILE_TABLE_MAPPING[filename]
    table_schema = TABLE_SCHEMA[filename]

    pandas_gbq.to_gbq(
        file,
        table_id,
        project_id=PROJECT_ID,
        if_exists='append',
        table_schema=table_schema
    )


def archive_zip_file(bucket_name: str, filename: str) -> None:
    current_bucket = google_cloud_storage_client.bucket(bucket_name)
    archive_bucket = google_cloud_storage_client.bucket(ARCHIVE_BUCKET)
    blob = current_bucket.blob(filename)
    current_bucket.copy_blob(blob, archive_bucket)
    blob.delete()


@functions_framework.cloud_event
def ingest_data(cloud_event: CloudEvent) -> None:
    logging.info(f"Cloud event received: {cloud_event!r}")
    data = cloud_event.data

    bucket = data["bucket"]
    name = data["name"]
    logging.info(f"Bucket: {bucket!r}, filename: {name!r}")

    zipped_file = read_zip_file_from_gcs(bucket, name)
    logging.info('Zip file retrieved from GCS')

    files = unzip_files(zipped_file)
    logging.info('CSV files unzipped and decrypted from zip file')

    for filename, file in files.items():
        logging.info(f'Processing of CSV file {filename!r} started')
        load_file_on_big_query(filename, file)
        logging.info(f'Processing of CSV file {filename!r} finished')

    archive_zip_file(bucket, name)
    logging.info(f'Zip file archived in {ARCHIVE_BUCKET!r}')
