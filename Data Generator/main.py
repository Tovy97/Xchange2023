import logging
import os
from datetime import datetime
from io import BytesIO
from random import randint, choice
from typing import *
from zipfile import ZipFile

import faker_commerce
from cryptography.fernet import Fernet
from faker import Faker
from geonamescache import GeonamesCache
from google.cloud.storage import Client as GoogleCloudStorageClient
from pandas import DataFrame

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

BUCKET_NAME: str = 'xchange-23'
FILE_PASSWORD: bytes = b'7B-H6CYVB8NHOc7obTEq3Wf7IecLSNc5awzGa8H_6zg='

ORDER_NUMBER: int = 5000
MAX_ROW_FOR_ORDER: int = 20

fake: Faker = Faker()
fake.add_provider(faker_commerce.Provider)

local_fake: Faker = Faker([
    'cs_CZ', 'da_DK', 'de_AT', 'de_CH', 'de_DE', 'en', 'en_GB', 'en_IE', 'en_IN', 'en_NZ', 'en_TH', 'en_US', 'es',
    'es_AR', 'es_CA', 'es_CL', 'es_CO', 'es_ES', 'es_MX', 'et_EE', 'fi_FI', 'fr_BE', 'fr_CA', 'fr_CH', 'fr_FR',
    'ga_IE', 'hr_HR', 'hu_HU', 'id_ID', 'it_IT', 'lt_LT', 'lv_LV', 'nl_BE', 'nl_NL', 'no_NO', 'pl_PL', 'pt_BR',
    'pt_PT', 'ro_RO', 'sl_SI', 'sv_SE', 'tr_TR', 'tw_GH',
])

geo_names_cache: GeonamesCache = GeonamesCache()
fernet: Fernet = Fernet(FILE_PASSWORD)


def generate_fake_order_row(order_id: str) -> Tuple[Dict, float]:
    price = fake.pyfloat(
        left_digits=2,
        right_digits=2,
        positive=True,
        max_value=99.99,
        min_value=0.1
    )
    quantity = fake.pyint(
        min_value=1,
        max_value=10
    )
    total_price = round(quantity * price, 2)
    row = {
        'order_id': order_id,
        'product_name': fake.ecommerce_name(),
        'price_per_unit': price,
        'quantity': quantity,
        'total_price': total_price
    }
    return row, total_price


def generate_fake_order_rows(order_id: str) -> Tuple[List[Dict], float]:
    rows = []
    total_price = 0
    for j in range(randint(1, MAX_ROW_FOR_ORDER)):
        row, price = generate_fake_order_row(order_id)
        rows.append(row)
        total_price += price
    return rows, round(total_price, 2)


def generate_fake_order() -> Tuple[Dict, List[Dict]]:
    city_key = choice(list(geo_names_cache.get_cities().keys()))
    city_dict = geo_names_cache.get_cities()[city_key]
    country_dict = geo_names_cache.get_countries()[city_dict['countrycode']]
    country_lang = set(country_dict['languages'].replace('-', '_').split(','))
    intersection = set(local_fake.locales).intersection(country_lang)
    if intersection != set():
        lang = choice(list(intersection))
        func_local_fake = local_fake[lang]
    else:
        func_local_fake = local_fake
    order = {
        'order_id': fake.unique.pystr(
            min_chars=10,
            max_chars=10
        ),
        'customer_name': func_local_fake.name(),
        'order_date': fake.date_between(
            start_date="-10y"
        ),
        'city': city_dict['name'],
        'country': country_dict['iso'],
        'currency': country_dict['currencycode']
    }
    rows_of_order, total_price = generate_fake_order_rows(order['order_id'])
    order['price'] = total_price
    return order, rows_of_order


def generate_fake_orders() -> Tuple[DataFrame, DataFrame]:
    orders = []
    rows_of_orders = []
    for i in range(ORDER_NUMBER):
        order, rows_of_order = generate_fake_order()
        orders.append(order)
        rows_of_orders.extend(rows_of_order)

    return DataFrame(
        orders,
        columns=[
            'order_id',
            'customer_name',
            'price',
            'currency',
            'order_date',
            'city',
            'country'
        ]
    ), DataFrame(
        rows_of_orders,
        columns=[
            'order_id',
            'product_name',
            'price_per_unit',
            'quantity',
            'total_price'
        ]
    )


def cypher_dataframe(dataframe: DataFrame) -> DataFrame:
    return dataframe.apply(lambda x: x.astype(str)).applymap(lambda x: fernet.encrypt(x.encode('utf-8')))


def get_files() -> Tuple[BytesIO, BytesIO]:
    orders = BytesIO()
    rows_of_orders = BytesIO()

    df_orders, df_rows_of_orders = generate_fake_orders()

    cypher_dataframe(df_orders).to_csv(orders, index=False)
    cypher_dataframe(df_rows_of_orders).to_csv(rows_of_orders, index=False)
    return orders, rows_of_orders


def zip_files(orders: BytesIO, rows_of_orders: BytesIO) -> BytesIO:
    zipped_file = BytesIO()
    with ZipFile(zipped_file, mode="w") as archive:
        archive.writestr('orders.csv', orders.getvalue())
        archive.writestr('rows_of_orders.csv', rows_of_orders.getvalue())
    return zipped_file


def get_filename() -> str:
    now = datetime.now().strftime('%Y_%m_%d-%H_%M_%S-%f')
    return f'orders_to_ingest-{now}.zip'


def write_on_disk(filename: str, zipped_file: BytesIO) -> None:
    os.makedirs('output/', exist_ok=True)
    zipped_file.seek(0)
    with open(f'output/{filename}', 'wb') as file:
        file.write(zipped_file.getvalue())


def write_on_gcs(filename: str, zipped_file: BytesIO) -> None:
    zipped_file.seek(0)
    google_cloud_storage_client = GoogleCloudStorageClient()
    bucket = google_cloud_storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(filename)
    blob.upload_from_file(zipped_file)


def main() -> None:
    try:
        orders, rows_of_orders = get_files()
        logging.info("CSV files generated")
        zipped_file = zip_files(orders, rows_of_orders)
        logging.info("CSV files zipped")
        filename = get_filename()
        logging.info("Filename generated")
        write_on_disk(filename, zipped_file)
        logging.info("Zip wrote on local disk")
        write_on_gcs(filename, zipped_file)
        logging.info("Zip wrote on GCS bucket")
    except Exception as e:
        logging.exception(f"Script failed with {str(e)!r}")


if __name__ == '__main__':
    logging.info("Script started")
    main()
    logging.info("Script ended")
