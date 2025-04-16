import json
import os

import pandas as pd
from geopy import Nominatim
import geopy
from geopy.geocoders import ArcGIS
import psycopg2
from sqlalchemy import create_engine
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeCheckOperator

NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'


def get_wikipedia_page(url):
    import requests

    print("Getting wikipedia page...", url)

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # check if the request is successful

        return response.text
    except requests.RequestException as e:
        print(f"An error occured: {e}")


def get_wikipedia_data(html):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find_all("table", {"class": "wikitable"})[1]

    table_rows = table.find_all('tr')

    return table_rows

def extract_wikipedia_data(**kwargs):
    url = kwargs['url']
    html = get_wikipedia_page(url)
    rows = get_wikipedia_data(html)

    data = []

    for i in range(1, len(rows)):
        tds = rows[i].find_all('td')
        values = {
            'rank': i,
            # Lấy data thô không qua xử lý
            'stadium': tds[0].text,
            'capacity': tds[1].text,
            'region': tds[2].text,
            'country': tds[3].text,
            'city': tds[4].text,
            'images': 'https://' + tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else "NO_IMAGE",
            'home_team': tds[6].text,
        }
        data.append(values)

    json_rows = json.dumps(data)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)

    return data
    # for i in range(1, len(rows)):
    #     tds = rows[i].find_all('td')
    #     values = {
    #         'rank': i,
    #         'stadium': clean_text(tds[0].text),
    #         'capacity': clean_text(tds[1].text).replace(',', '').replace('.', ''),
    #         'region': clean_text(tds[2].text),
    #         'country': clean_text(tds[3].text),
    #         'city': clean_text(tds[4].text),
    #         'images': 'https://' + tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else "NO_IMAGE",
    #         'home_team': clean_text(tds[6].text),
    #     }
def clean_text(text):
    text = str(text).strip()
    text = text.replace('&nbsp', '')
    if text.find(' ♦'):
        text = text.split(' ♦')[0]
    if text.find('[') != -1:
        text = text.split('[')[0]
    if text.find(' (formerly)') != -1:
        text = text.split(' (formerly)')[0]

    return text.replace('\n', '')

def get_lat_long(country, city):
    geolocator = ArcGIS(timeout=5)
    location = geolocator.geocode(f'{city}, {country}')

    print(f"📍 Searching for: {city}, {country}")
    print(f"🔎 Location found: {location}")

    if location:
        return location.latitude, location.longitude

    return None


def transform_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')

    data = json.loads(data)

    stadiums_df = pd.DataFrame(data)
    text_columns = ['stadium', 'region', 'country', 'city', 'home_team']
    for col in text_columns:
        stadiums_df[col] = stadiums_df[col].apply(clean_text)

    stadiums_df['capacity'] = (
        stadiums_df['capacity']
        .apply(clean_text)
        .str.replace(',', '')
        .str.replace('.', '')
        .astype(int)
    )
    stadiums_df['location'] = stadiums_df.apply(lambda x: get_lat_long(x['country'], x['stadium']), axis=1)
    stadiums_df['images'] = stadiums_df['images'].apply(lambda x: x if x not in ['NO_IMAGE', '', None] else NO_IMAGE)

    # handle the duplicates
    duplicates = stadiums_df[stadiums_df.duplicated(['location'])]
    duplicates['location'] = duplicates.apply(lambda x: get_lat_long(x['country'], x['city']), axis=1)
    stadiums_df.update(duplicates)

    # push to xcom
    kwargs['ti'].xcom_push(key='rows', value=stadiums_df.to_json())

    return "OK"


def write_wikipedia_data(**kwargs):
    from datetime import datetime
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')

    print("DEBUG: Data pulled from XCom:", data)  # Kiểm tra XCom có dữ liệu không

    if not data:
        raise ValueError("No data found in XCom from transform_wikipedia_data!")

    data = json.loads(data)
    data = pd.DataFrame(data)

    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))    # Thư mục của file hiện tại
    save_dir = os.path.join(base_dir, "data")  # Tạo thư mục `data` trong project

    # Tạo thư mục nếu chưa có
    os.makedirs(save_dir, exist_ok=True)

    file_name = ('stadium_cleaned_' + str(datetime.now().date())
                 + "_" + str(datetime.now().time()).replace(":", "_") + '.csv')

    file_path = os.path.join(save_dir, file_name)

    # data.to_csv('data/' + file_name, index=False)
    data.to_csv(file_path, index=False)

    print("DEBUG: Saving file:", file_path)
def write_wikipedia_data(**kwargs):
    # Lấy dữ liệu từ XCom
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')

    if not data:
        raise ValueError("❌ No data found in XCom from transform_wikipedia_data!")

    # Chuyển dữ liệu từ JSON sang DataFrame
    data = json.loads(data)
    df = pd.DataFrame(data)

    postgres_hook = PostgresHook(postgres_conn_id='stadiums_connection')
    insert_query = """
        INSERT INTO stadiums (rank, stadium, capacity, region, country, city, images, home_team)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (Rank) DO NOTHING;
        """
    for _, row in df.iterrows():
        postgres_hook.run(
            insert_query,
            parameters=(
                row['rank'],
                row['stadium'],
                row['capacity'],
                row['region'],
                row['country'],
                row['city'],
                row['images'],
                row['home_team']
            )
        )

# def write_wikipedia_data(**kwargs):
#     # Lấy dữ liệu từ XCom
#     data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')
#
#     if not data:
#         raise ValueError("❌ No data found in XCom from transform_wikipedia_data!")
#
#     # Chuyển dữ liệu từ JSON sang DataFrame
#     data = json.loads(data)
#     df = pd.DataFrame(data)
#
#     # Kết nối Snowflake
#     snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
#
#     insert_query = """
#         INSERT INTO stadiums (rank, stadium, capacity, region, country, city, images, home_team)
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
#     """
#
#     # Đẩy dữ liệu lên Snowflake
#     for _, row in df.iterrows():
#         snowflake_hook.run(
#             insert_query,
#             parameters=(
#                 row['rank'],
#                 row['stadium'],
#                 row['capacity'],
#                 row['region'],
#                 row['country'],
#                 row['city'],
#                 row['images'],
#                 row['home_team']
#             )
#         )
