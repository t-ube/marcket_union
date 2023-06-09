import os
import json
import httpx
import postgrest
import datetime
import glob
import time
import copy
import pandas as pd
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

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def getInsertQuery2LongChart(id_list:list[str]):
    id_list_str = ', '.join(f"'{id}'" for id in id_list)
    query = f"""
        with tdexp AS (
        SELECT master_id FROM card_base WHERE master_id IN ({id_list_str})
        ),
        td1 AS (
        SELECT td0.master_id, td0.min, td0.percent_50 AS p50, to_char(timezone('JST',td0.datetime::timestamptz),'YYYY-MM-DD') AS date,
        td0.datetime, td0.count AS stock
        FROM card_price_daily AS td0
        INNER JOIN tdexp ON td0.master_id = tdexp.master_id
        WHERE now() - interval '294days' < td0.datetime AND td0.min IS NOT NULL
        ),
        td2 AS (
        SELECT t1.master_id, t1.min, t1.p50, t1.date, t1.datetime, t1.stock,
        CASE WHEN t2.use_count IS NULL THEN 0 ELSE t2.use_count END AS use_count FROM td1 AS t1
        LEFT JOIN event_use_card_daily AS t2 ON t1.master_id = t2.master_id AND t1.date = to_char(t2.date,'YYYY-MM-DD')
        ORDER BY datetime
        ),
        td3 AS (
        SELECT master_id, jsonb_agg(json_build_object('master_id', master_id, 'min', min, 'p50', p50, 'date', date, 'stock', stock, 'use_count', use_count) ORDER BY datetime) AS charts FROM td2
        GROUP BY master_id
        )
        INSERT INTO card_market_chart_long(master_id, chart_line) 
        SELECT master_id, charts FROM td3
        ON CONFLICT (master_id) DO UPDATE SET chart_line = EXCLUDED.chart_line, updated_at = now();
    """
    return query

# 全カードを取得する
dfCards = loadCardDf()
id_list = []
for index, row in dfCards.iterrows():
    if pd.isnull(row['master_id']):
        print('skip:'+row['name'])
        continue
    id_list.append(row['master_id'])

id_list_chunks = list(chunks(id_list, 200))

for id_chunk in id_list_chunks:
    print(getInsertQuery2LongChart(id_chunk))
