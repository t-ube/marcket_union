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

def getInsertQuery2Daily(master_id:str):
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

def getInsertQuery2Latest(master_id:str):
    query = "WITH td1 AS ("
    query += " SELECT (current_date - n * interval '1 day')::timestamp AT TIME ZONE 'Asia/Tokyo' AS datetime"
    query += " FROM generate_series(0, 7) AS n"
    query += " ORDER BY datetime"
    query += " ),"
    query += " td2 AS ("
    query += " SELECT "
    query += " '"+master_id+"' AS master_id,"
    query += " td1.datetime,"
    query += " COALESCE(t.count, 0) AS stock,"
    query += " t.min,"
    query += " t.percent_50,"
    query += " t.max"
    query += " FROM td1 LEFT JOIN card_price_daily AS t "
    query += " ON td1.datetime=t.datetime AND t.master_id='"+master_id+"'"
    query += " ),"
    query += " td3 AS ("
    query += " SELECT *,"
    query += " lag(stock, 1) over (order by datetime) as stock_before_1d,"
    query += " lag(percent_50, 1) over (order by datetime) as p50_before_1d,"
    query += " lag(percent_50, 7) over (order by datetime) as p50_before_7d,"
    query += " lag(min, 1) over (order by datetime) as min_before_1d,"
    query += " lag(min, 7) over (order by datetime) as min_before_7d"
    query += " FROM td2"
    query += " ),"
    query += " td4 AS ("
    query += " SELECT "
    query += " master_id,"
    query += " datetime,"
    query += " percent_50 AS p50,"
    query += " min,"
    query += " stock,"
    query += " stock - stock_before_1d AS gap_1d_stock,"
    query += " percent_50 - p50_before_1d AS gap_p50_1d,"
    query += " CASE WHEN p50_before_1d IS NULL THEN 0.0 ELSE ((percent_50 / p50_before_1d) - 1) * 100 END AS ratio_p50_1d,"
    query += " percent_50 - p50_before_7d AS gap_p50_7d,"
    query += " CASE WHEN p50_before_1d IS NULL THEN 0.0 ELSE ((percent_50 / p50_before_7d) - 1) * 100 END AS ratio_p50_7d"
    query += " FROM td3"
    query += " ),"
    query += " td5 AS ("
    query += " SELECT"
    query += " master_id,"
    query += " MAX(datetime) AS datetime,"
    query += " SUM(CASE WHEN gap_1d_stock < 0 THEN 1 ELSE 0 END) AS days_decreas_stock_7d,"
    query += " SUM(gap_1d_stock) AS total_gap_stock_7d"
    query += " FROM td4 GROUP BY master_id"
    query += " )"
    query += " INSERT INTO card_market_latest_price"
    query += " ("
    query += " master_id,"
    query += " datetime,"
    query += " p50,"
    query += " stock,"
    query += " gap_1d_stock,"
    query += " gap_p50_1d,"
    query += " ratio_p50_1d,"
    query += " gap_p50_7d,"
    query += " ratio_p50_7d,"
    query += " days_decreas_stock_7d,"
    query += " total_gap_stock_7d"
    query += " )"
    query += " SELECT "
    query += " td4.master_id,"
    query += " td4.datetime,"
    query += " td4.p50,"
    query += " td4.stock,"
    query += " td4.gap_1d_stock,"
    query += " td4.gap_p50_1d,"
    query += " ROUND(td4.ratio_p50_1d) AS ratio_p50_1d,"
    query += " td4.gap_p50_7d,"
    query += " ROUND(td4.ratio_p50_7d) AS ratio_p50_7d,"
    query += " td5.days_decreas_stock_7d,"
    query += " td5.total_gap_stock_7d"
    query += " FROM td4 INNER JOIN td5 ON td4.datetime = td5.datetime"
    query += " ON CONFLICT (master_id) DO NOTHING;"
    return query

def getInsertQuery2Chart(master_id:str):
    query = "WITH td1 AS ("
    query += " SELECT (current_date - n * interval '1 day')::timestamp AT TIME ZONE 'Asia/Tokyo' AS datetime_jst"
    query += " FROM generate_series(0, 6) AS n"
    query += " ), td2 AS ("
    query += " SELECT "
    query += " master_id,"
    query += " datetime AT TIME ZONE 'Asia/Tokyo' AS datetime_jst, "
    query += " COALESCE(min, 0) AS min,"
    query += " COALESCE(percent_50, 0) AS percent_50, "
    query += " COALESCE(count, 0) AS stock"
    query += " FROM card_price_daily"
    query += " WHERE master_id = '"+master_id+"'"
    query += " AND datetime >= current_date::timestamp - interval '7 day' "
    query += " AND datetime <= current_date::timestamp"
    query += " )"
    query += " INSERT INTO card_market_daily_price_chart ("
    query += " master_id,"
    query += " price_list_7d"
    query += " )"
    query += " SELECT"
    query += " '"+master_id+"',"
    query += " jsonb_build_object("
    query += " 'start', (SELECT percent_50 FROM td2 WHERE percent_50 IS NOT NULL ORDER BY datetime_jst ASC LIMIT 1),"
    query += " 'end', (SELECT percent_50 FROM td2 WHERE percent_50 IS NOT NULL ORDER BY datetime_jst DESC LIMIT 1),"
    query += " 'min', (SELECT min(percent_50) FROM td2),"
    query += " 'max', (SELECT max(percent_50) FROM td2),"
    query += " 'items', jsonb_agg("
    query += " jsonb_build_object("
    query += " 'datetime', TO_CHAR(td1.datetime_jst, 'YYYY-MM-DD 00:00:00'),"
    query += " 'min', td2.min,"
    query += " 'p50', td2.percent_50,"
    query += " 'stock', td2.stock"
    query += " ) ORDER BY td1.datetime_jst ASC"
    query += " )"
    query += " ) AS price_list_7d"
    query += " FROM td1 LEFT JOIN td2 ON td1.datetime_jst::date = td2.datetime_jst::date"
    query += " ON CONFLICT (master_id) DO NOTHING;"
    return query

def getInsertQuery2IPI():
    query = "INSERT INTO card_info_price_inventory"
    query += " ("
    query += " master_id, summary, datetime, p50, stock,"
    query += " gap_1d_stock, gap_p50_1d, ratio_p50_1d,"
    query += " gap_p50_7d, ratio_p50_7d, days_decreas_stock_7d, total_gap_stock_7d,"
    query += " price_list_7d"
    query += " )"
    query += " SELECT "
    query += " t1.master_id,t1.summary,t2.datetime, t2.p50, t2.stock,"
    query += " t2.gap_1d_stock, t2.gap_p50_1d, t2.ratio_p50_1d,"
    query += " t2.gap_p50_7d, t2.ratio_p50_7d, t2.days_decreas_stock_7d, t2.total_gap_stock_7d,"
    query += " t3.price_list_7d"
    query += " FROM card_base AS t1"
    query += " LEFT JOIN card_market_latest_price AS t2 ON t1.master_id=t2.master_id"
    query += " LEFT JOIN card_market_daily_price_chart AS t3 ON t1.master_id=t3.master_id"
    query += " ON CONFLICT (master_id) DO NOTHING;"
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

# 削除
cursor.execute('DELETE FROM card_market_latest_price;')
connection.commit()

cursor.execute('DELETE FROM card_market_daily_price_chart;')
connection.commit()

for id in id_list:
    query = getInsertQuery2Daily(id)
    cursor.execute(query)
    connection.commit()

for id in id_list:
    query = getInsertQuery2Latest(id)
    cursor.execute(query)
    connection.commit()

for id in id_list:
    query = getInsertQuery2Chart(id)
    cursor.execute(query)
    connection.commit()

cursor.execute('DELETE FROM card_info_price_inventory;')
connection.commit()
cursor.execute(getInsertQuery2IPI())
connection.commit()

cursor.close()
connection.close()
