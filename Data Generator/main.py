import os
from random import randint, choice
from typing import *

import faker_commerce
from faker import Faker
from geonamescache import GeonamesCache
from pandas import DataFrame

ORDER_NUMBER = 5000
MAX_ROW_FOR_ORDER = 20

fake: Faker = Faker()
fake.add_provider(faker_commerce.Provider)

local_fake: Faker = Faker([
    'cs_CZ', 'da_DK', 'de_AT', 'de_CH', 'de_DE', 'en', 'en_GB', 'en_IE', 'en_IN', 'en_NZ', 'en_TH', 'en_US', 'es',
    'es_AR', 'es_CA', 'es_CL', 'es_CO', 'es_ES', 'es_MX', 'et_EE', 'fi_FI', 'fr_BE', 'fr_CA', 'fr_CH', 'fr_FR',
    'ga_IE', 'hr_HR', 'hu_HU', 'id_ID', 'it_IT', 'lt_LT', 'lv_LV', 'nl_BE', 'nl_NL', 'no_NO', 'pl_PL', 'pt_BR',
    'pt_PT', 'ro_RO', 'sl_SI', 'sv_SE', 'tr_TR', 'tw_GH',
])

geo_names_cache = GeonamesCache()


def generate_fake_order_row(order_id) -> Tuple[Dict, float]:
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


def generate_fake_order_rows(order_id) -> Tuple[List[Dict], float]:
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


if __name__ == '__main__':
    os.makedirs('output/', exist_ok=True)
    df_orders, df_rows_of_orders = generate_fake_orders()
    df_orders.to_csv('output/orders.csv', index=False)
    df_rows_of_orders.to_csv('output/rows_of_orders.csv', index=False)
