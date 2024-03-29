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

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

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
    query += " total_gap_stock_7d,"
    query += " min"
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
    query += " td5.total_gap_stock_7d,"
    query += " td4.min"
    query += " FROM td4 INNER JOIN td5 ON td4.datetime = td5.datetime"
    query += " ON CONFLICT (master_id) DO NOTHING;"
    return query

def getInsertQuery2Chart(master_id:str):
    query = "WITH td1 AS ("
    query += " SELECT ((current_date::timestamp - n * interval '1 day')::timestamp AT TIME ZONE 'UTC')::date AS date"
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
    query += " 'datetime', TO_CHAR(td1.date, 'YYYY-MM-DD 00:00:00'),"
    query += " 'min', td2.min,"
    query += " 'p50', td2.percent_50,"
    query += " 'stock', td2.stock"
    query += " ) ORDER BY td2.datetime_jst ASC"
    query += " )"
    query += " ) AS price_list_7d"
    query += " FROM td1 LEFT JOIN td2 ON td1.date = td2.datetime_jst::date"
    query += " ON CONFLICT (master_id) DO NOTHING;"
    return query

def getInsertQuery2UsageDaily():
    query = """
        WITH td1 AS ( 
        WITH 
        temp AS (SELECT * FROM event_deck_item WHERE date > current_date - interval '30 days'),
        total_count AS (SELECT SUM(count) as total_count FROM temp),
        unique_decks AS (SELECT COUNT(DISTINCT deck_id) as unique_decks FROM temp)
        SELECT 
        temp.master_id,
        SUM(temp.count) as total_cards,
        COUNT(DISTINCT temp.deck_id) as unique_decks_per_card,
        SUM(temp.count)::FLOAT / total_count.total_count * 100 as count_ratio,
        COUNT(DISTINCT temp.deck_id)::FLOAT / unique_decks.unique_decks * 100 as deck_ratio
        FROM 
        temp, total_count, unique_decks 
        WHERE 
        total_count.total_count > 0 AND unique_decks.unique_decks > 0
        GROUP BY 
        temp.master_id, total_count.total_count, unique_decks.unique_decks
        )
        INSERT INTO card_usage_daily (master_id, datetime, total_cards, unique_decks_per_card, count_ratio, deck_ratio)
        SELECT 
        master_id, current_date::timestamp AT TIME ZONE 'Asia/Tokyo', total_cards, unique_decks_per_card, count_ratio, deck_ratio
        FROM td1
        ON CONFLICT (master_id,datetime) DO NOTHING;
    """
    return query

def getInsertQuery2Usage():
    query = """
    WITH td1 AS (
    SELECT
    master_id,
    total_cards,
    unique_decks_per_card,
    count_ratio,
    deck_ratio,
    total_cards - previous_total_cards AS total_cards_change,
    total_cards / unique_decks_per_card AS avg_cards_per_decks,
    deck_ratio - previous_deck_ratio AS deck_ratio_change
    FROM (
    SELECT
    t1.master_id,
    t1.deck_ratio,
    t2.deck_ratio AS previous_deck_ratio,
    t1.total_cards,
    t2.total_cards AS previous_total_cards,
    t1.unique_decks_per_card,
    t2.unique_decks_per_card AS previous_unique_decks_per_card,
    t1.count_ratio
    FROM card_usage_daily t1
    INNER JOIN card_usage_daily t2
        ON t1.master_id = t2.master_id
        AND t1.datetime = (SELECT MAX(datetime) FROM card_usage_daily)
        AND t2.datetime = (SELECT MAX(datetime) - INTERVAL '1 day' FROM card_usage_daily)
    ) AS subquery)
    INSERT INTO card_usage (master_id, total_cards, unique_decks_per_card, count_ratio, deck_ratio, total_cards_change, avg_cards_per_decks, deck_ratio_change)
    SELECT 
    master_id, total_cards, unique_decks_per_card, count_ratio, deck_ratio, total_cards_change, avg_cards_per_decks, deck_ratio_change
    FROM td1
    ON CONFLICT (master_id) DO NOTHING;
    """
    return query

def getInsertQuery2IPI():
    query = """
        INSERT INTO card_info_price_inventory
        (
        master_id, summary, datetime, p50, stock,
        gap_1d_stock, gap_p50_1d, ratio_p50_1d,
        gap_p50_7d, ratio_p50_7d, days_decreas_stock_7d, total_gap_stock_7d,
        price_list_7d,
        usage_total, unique_decks_per_card, usage_count_ratio, deck_ratio,
        min,
        usage_total_change, avg_cards_per_decks, deck_ratio_change
        )
        SELECT 
        t1.master_id,t1.summary,t2.datetime, t2.p50, t2.stock,
        t2.gap_1d_stock, t2.gap_p50_1d, t2.ratio_p50_1d,
        t2.gap_p50_7d, t2.ratio_p50_7d, t2.days_decreas_stock_7d, t2.total_gap_stock_7d,
        t3.price_list_7d,
        t4.total_cards AS usage_total, t4.unique_decks_per_card, t4.count_ratio AS usage_count_ratio, t4.deck_ratio,
        t2.min,
        t4.total_cards_change AS usage_total_change, t4.avg_cards_per_decks, t4.deck_ratio_change
        FROM card_base AS t1
        LEFT JOIN card_market_latest_price AS t2 ON t1.master_id=t2.master_id
        LEFT JOIN card_market_daily_price_chart AS t3 ON t1.master_id=t3.master_id
        LEFT JOIN card_usage AS t4 ON t1.master_id=t4.master_id
        ON CONFLICT (master_id) DO NOTHING;
    """
    return query

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

cursor.execute('DELETE FROM card_usage;')
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

# 200行単位でロングチャートを生成する
id_list_chunks = list(chunks(id_list, 200))
for id_chunk in id_list_chunks:
    query = getInsertQuery2LongChart(id_chunk)
    cursor.execute(query)
    connection.commit()

cursor.execute(getInsertQuery2UsageDaily())
connection.commit()
cursor.execute(getInsertQuery2Usage())
connection.commit()

# 最終集計
cursor.execute('DELETE FROM card_info_price_inventory;')
connection.commit()
cursor.execute(getInsertQuery2IPI())
connection.commit()

cursor.close()
connection.close()
