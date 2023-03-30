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
from supabase import create_client, Client 
from scripts import jst
from scripts import marcketCalc
from scripts import marcketPrice
from scripts import supabaseUtil

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
    print(readDfAll[readDfAll.duplicated(subset=['master_id'], keep=False)])
    return readDfAll

# 従来ファイルと同等フォーマットへの変換処理
def convertDailyPriceRecordData(rec):
    return {
        'datetime':rec['datetime_jst'],
        'count':rec['count'],
        'mean':rec['mean'],
        'std':rec['std'],
        'min':rec['min'],
        '25%':rec['percent_25'],
        '50%':rec['percent_50'],
        '75%':rec['percent_75'],
        'max':rec['max']
    }

# 1週間分のデータを取得する。
def getWeeklyData(ioCsv, currentDT):
    firstDate = currentDT - datetime.timedelta(days=7)
    rangeDf = pd.DataFrame(index=pd.date_range(
        firstDate.strftime('%Y-%m-%d'),
        currentDT.strftime('%Y-%m-%d')))
    dfCsv = ioCsv.getDataframe()
    d7Df = pd.merge(rangeDf,dfCsv,how='outer',left_index=True,right_index=True)
    d7Df = d7Df.replace(0, {'count': None})
    fillDf = d7Df.interpolate('ffill')
    formatDf = fillDf.asfreq('1D', method='ffill').fillna(0).tail(7)
    #print(formatDf)
    return formatDf

# 半年分のデータを取得する。（2週間間隔）
def getHalfYearData(ioCsv, currentDT):
    firstDate = currentDT - datetime.timedelta(days=168)
    rangeDf = pd.DataFrame(index=pd.date_range(
        firstDate.strftime('%Y-%m-%d'),
        currentDT.strftime('%Y-%m-%d')))
    dfCsv = ioCsv.getDataframe()
    d168Df = pd.merge(rangeDf,dfCsv,how='outer',left_index=True,right_index=True)
    d168Df = d168Df.replace(0, {'count': None})
    fillDf = d168Df.interpolate('ffill')
    formatDf = fillDf.asfreq('14D', method='ffill').fillna(0)
    #print(formatDf)
    return formatDf


url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_ANON_KEY")
service_key: str = os.environ.get("SUPABASE_SERVICE_KEY")
supabase_uri: str = os.environ.get("SUPABASE_URI")

supabase: Client = create_client(url, key)
supabase.postgrest.auth(service_key)

dailyPriceReader = supabaseUtil.CardPriceDailyReader()
editor = supabaseUtil.batchEditor()
writer = supabaseUtil.batchWriter()
cleaner = supabaseUtil.shopItemCleaner()

currentDT = jst.now()
print(currentDT)

if os.path.exists('./daily') == False:
    Path('./daily').mkdir(parents=True, exist_ok=True)

# 全カードを取得する
dfCards = loadCardDf()
id_list = []
for index, row in dfCards.iterrows():
    if pd.isnull(row['master_id']):
        print('skip:'+row['name'])
        continue
    id_list.append(row['master_id'])

for i in range(0, len(id_list), 30):
    batch = id_list[i: i+30]
    print('Write log no.:'+str(i))
    data1 = dailyPriceReader.readLimit(supabase,batch,currentDT)

    # 日次記録をCSVファイルに書き込む
    if len(data1) > 0:
        df = pd.DataFrame.from_records(data1)
        for master_id in batch:
            cardDf = df[df['master_id'] == master_id]
            dataDir = './daily/'+master_id
            if os.path.exists(dataDir) == False:
                Path(dataDir).mkdir(parents=True, exist_ok=True)
            dailyCsv = marcketPrice.dailyPriceIOCSV(dataDir)
            records = []
            for index, row in cardDf.iterrows():
                records.append(convertDailyPriceRecordData(row))
            recordDf = pd.DataFrame.from_records(records)
            dailyCsv.addPostgresData(recordDf)
            dailyCsv.save()

    batch_results = []
    for master_id in batch:
        dataDir = './daily/'+master_id
        if os.path.exists(dataDir) == False:
            Path(dataDir).mkdir(parents=True, exist_ok=True)
        
        # 日次情報（CSVファイルから読み込む）
        dailyCsv = marcketPrice.dailyPriceIOCSV(dataDir)
        dailyCsv.load()
        calc = marcketCalc.calc(currentDT.strftime('%Y-%m-%d'))
        recordDf = pd.DataFrame(columns=['market','link','price','name','date','datetime','stock'])
        recordDf = calc.convert2BaseDf(recordDf)
        days30Df = calc.getDailyDf2(recordDf,30)
        dailyCsv.add(days30Df)
        
        # 集計結果（Supabaseに記録する）
        # 1週間分のデータを取得する。（日間）
        daysDf = getWeeklyData(dailyCsv, currentDT)
        halfYearDf = getHalfYearData(dailyCsv, currentDT)
        # 最初と最後を抽出する
        sampleDf = pd.concat([daysDf.head(1), daysDf.tail(1)])
        batch_results.append(editor.getCardMarketResult(master_id,
            calc.getWriteDailyDf(
            None,
            daysDf.tail(1),
            sampleDf.diff().tail(1),
            daysDf,
            daysDf.diff(),
            halfYearDf,
            halfYearDf.diff())
        ))

    result1 = writer.write(supabase, "card_market_result", batch_results)
    # 削除
    if result1 == True:
        cleaner.delete(supabase,batch,currentDT)
