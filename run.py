import requests
import os
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from os.path import join, dirname
from dotenv import load_dotenv

ON_HEROKU = os.environ.get("ON_HEROKU", False)

DATABASE_URL = os.environ.get('DATABASE_URL')
if not ON_HEROKU:
    dotenv_path = join(dirname(__file__), '.env')
    load_dotenv(dotenv_path)
    DATABASE_URL = 'postgresql://@localhost:5432/kabudb'
DB_ENGINE = create_engine(DATABASE_URL)

file_path = '/tmp/%s_%s.xls' if ON_HEROKU else './%s_%s.xls'

TARGET_URL = 'https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls'
TODAY = datetime.today().strftime('%y%m%d')


def download_xls(url):
    res = requests.get(url)
    if res.status_code == 200:
        open(file_path % (TODAY, 'JPX'), 'wb').write(res.content)
        print('Done')


COLUMN_RULE = {
    '日付': 'date',
    'コード': 'code',
    '銘柄名': 'name',
    '市場・商品区分': 'category',
    '33業種コード': 'industry33code',
    '33業種区分': 'industry33category',
    '17業種コード': 'industry17code',
    '17業種区分': 'industry17category',
    '規模コード': 'sizecode',
    '規模区分': 'sizecategory'
}

download_xls(TARGET_URL)

df = pd.read_excel(file_path % (TODAY, 'JPX'), sheet_name='Sheet1', skiprows=0)
df.rename(columns=COLUMN_RULE, inplace=True)
df['id'] = df['code']
df.set_index('id', inplace=True)

df.to_sql('jpx2', DB_ENGINE, if_exists='replace')
