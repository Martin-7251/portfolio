import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import os
from geopy.geocoders import Nominatim
from datetime import datetime
import time

No_Image = 'https://en.m.wikipedia.org/wiki/File:No_image_available.svg'

def get_wikipedia_page(url):

    print("Getting Wikipedia Page...", url)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status() # check if the request is successful

        return response.text
    
    except requests.RequestException as e:
        print(f"An error occured: {e}")


def get_wikipedia_data(html):
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.select("table.wikitable.sortable") or soup.select("table.wikitable")

    if not table:
        raise ValueError("No sortable wikitable found on the Wikipedia page.")

    table = table[0]
    table_rows = table.find_all('tr')

    return table_rows

def clean_text(text):
    text = str(text).strip()

    if ' ♦' in text:
        text = text.replace(' ♦', '')
    
    if '[' in text:
        text = text.split('[')[0]

    if ' (formerly)' in text:
        text = text.split(' (formerly)')[0]

    if isinstance(text, tuple):
        text = ' '.join([str(t) for t in text])

    return text.replace('\n', '').strip()


def extract_wikipedia_data(**kwargs):
    url = kwargs['url']
    html = get_wikipedia_page(url)
    rows = get_wikipedia_data(html)

    data = []

    for i in range(1, len(rows)):
        tds = rows[i].find_all('td')
        values = {
            'rank': i,
            'stadium': clean_text(tds[0].text),
            'capacity': clean_text(tds[1].text).replace(',', ''),
            'region': clean_text(tds[2].text),
            'country': clean_text(tds[3].text),
            'city': clean_text(tds[4].text),
            'images': 'https://' + tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else "No_Image",
            'home_team': clean_text(tds[6].text),
        }

        data.append(values)

    data_df = pd.DataFrame(data)
    data_df.to_csv("/opt/airflow/data/output.csv", index=False)

    json_rows = json.dumps(data)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)

    return "OK"

geo_cache = {}

def get_lat_long(country, city):
    key = f"{city},{country}"
    if key in geo_cache:
        return geo_cache[key]
    geolocator = Nominatim(user_agent='FOOTBALL_PROJECT_DE (martinkamau72@gmail.com)', timeout=10)
    try:
        time.sleep(1)
        location = geolocator.geocode(key)
        lat_lng = (location.latitude, location.longitude) if location else (None, None)
        geo_cache[key] = lat_lng
        return lat_lng
    except Exception as e:
        print(f"Geocoding error for {key}: {e}")
        geo_cache[key] = (None, None)
        return (None, None)

def transform_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')
    df = pd.DataFrame(json.loads(data))

    # Geocode with caching
    df['location'] = df.apply(lambda x: get_lat_long(x['country'], x['stadium']), axis=1)

    # Clean up image links and capacity
    df['images'] = df['images'].apply(lambda x: x if x not in ['No_Image', '', None] else No_Image)
    df['capacity'] = pd.to_numeric(df['capacity'], errors='coerce').fillna(0).astype(int)

    # Remove duplicate locations and re-geocode
    mask_duplicates = df.duplicated(['location'])
    if mask_duplicates.any():
        duplicates = df[mask_duplicates].copy()
        duplicates['location'] = duplicates.apply(lambda x: get_lat_long(x['country'], x['city']), axis=1)
        df.update(duplicates)

    kwargs['ti'].xcom_push(key='rows', value=df.to_json())
    return "OK"

def write_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')

    data = json.loads(data)

    data = pd.DataFrame(data)

    file_name=('stadium_cleaned' + str(datetime.now().date()) + "_" + str(datetime.now().time()).replace(":", "_") + '.csv')

    # data.to_csv('data/' + file_name, index=False)

    data.to_csv('abfs://footballdatamk@footballdataengmk.dfs.core.windows.net/data/' + file_name, 
                storage_options={'account_key': 'uyuggnhh8/RNgNsceOxA6YsceRts8pX81lU9ExzZ0lSQ4AWUvrUHT8DPaRB0iqcNl1WOhGKkg2Sg+AStGtjtKg=='

                }, index=False)



