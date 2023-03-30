import os
import json
import httpx
import postgrest
import datetime
import glob
import time
import copy
import pandas as pd
import psycopg2
from pathlib import Path
from uuid import UUID

# カードのデータフレームを取得
def loadCardDf():
    card_list = []
    files = glob.glob("./card/*.csv")
    for file in files:
        readDf = pd.read_csv(
            file,
            encoding="utf_8_sig", sep=",",
            header=0)
        card_list.append(readDf)
    readDfAll = pd.concat(card_list, axis=0, ignore_index=True, sort=True)
    return readDfAll

def getInsertQuery(master_id:str):
    query = "WITH td AS ("
    query += " SELECT "
    query += " master_id,"
    query += " datetime,"
    query += " link,"
    query += " id,"
    query += " created_at,"
    query += " updated_at,"
    query += " date,"
    query += " shop_name,"
    query += " item_name,"
    query += " price,"
    query += " generate_series(1, stock) AS stock"
    query += " FROM shop_item"
    query += " ),"
    query += " td2 AS ("
    query += " SELECT "
    query += " master_id,"
    query += " date AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Tokyo' AS datetime,"
    query += " COUNT(*) AS count,"
    query += " CAST(round(AVG(price),4) AS float) AS mean,"
    query += " CAST(round(STDDEV_SAMP(price),4) AS float) AS std,"
    query += " MIN(price) AS min,"
    query += " percentile_cont(0.25) WITHIN GROUP (ORDER BY price) AS percent_25,"
    query += " percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS percent_50,"
    query += " percentile_cont(0.75) WITHIN GROUP (ORDER BY price) AS percent_75,"
    query += " MAX(price) AS max"
    query += " FROM ("
    query += " SELECT "
    query += " master_id,"
    query += " price,"
    query += " date,"
    query += " ntile(4) OVER (ORDER BY price) AS price_ntile"
    query += " FROM td"
    query += " WHERE stock > 0 AND master_id='"+master_id+"'"
    query += " ) t"
    query += " WHERE price_ntile IN (1, 2, 3, 4) GROUP BY master_id, date"
    query += " )"
    query += " INSERT INTO card_price_daily"
    query += " ("
    query += " master_id,"
    query += " datetime,"
    query += " updated_at,"
    query += " count,"
    query += " mean,"
    query += " std,"
    query += " min,"
    query += " percent_25,"
    query += " percent_50,"
    query += " percent_75,"
    query += " max"
    query += " )"
    query += " SELECT "
    query += " td2.master_id,"
    query += " td2.datetime,"
    query += " now() AS updated_at,"
    query += " CASE WHEN td2.count > 0 THEN td2.count ELSE 0 END AS count,"
    query += " td2.mean, td2.std, td2.min, td2.percent_25, td2.percent_50, td2.percent_75, td2.max"
    query += " FROM td2"
    query += " ON CONFLICT (master_id,datetime) DO NOTHING;"
    return query

supabase_uri: str = os.environ.get("SUPABASE_URI")

# 全カードを取得する
dfCards = loadCardDf()
id_list = []
for index, row in dfCards.iterrows():
    if pd.isnull(row['master_id']):
        print('skip:'+row['name'])
        continue
    id_list.append(row['master_id'])

# 全カードの集計を行う
connection = psycopg2.connect(supabase_uri, sslmode='require')
cursor = connection.cursor()

for id in id_list:
    query = getInsertQuery(id)
    cursor.execute(query)
    connection.commit()

cursor.close()
connection.close()
