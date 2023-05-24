import os
import json
import httpx
import postgrest
import datetime
import glob
import time
import copy
import numpy as np
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
    readDfAll = readDfAll.replace([np.inf, -np.inf], 0.0)
    readDfAll = readDfAll.fillna('n/a')
    print(readDfAll[readDfAll.duplicated(subset=['master_id'], keep=False)])
    return readDfAll

# レギュレーション更新
def getNewRegulationDf(df:pd.DataFrame):
    # 同名扱いのカードはこのフラグで吸収
    df['hakase'] = False
    df['boss'] = False

    df_modified = df.copy()
    df_modified = df_modified[df_modified['card_type'] == 'トレーナーズ']
    df_modified = df_modified[df_modified['regulation'] != '-']
    df_modified.loc[df_modified['name'].str.contains('博士の研究'),'hakase'] = True
    df_modified.loc[df_modified['name'].str.contains('ボスの指令'),'boss'] = True
    df_modified = df_modified.sort_values(by=['regulation'], ascending=[True])

    df_temp = df_modified[~df_modified.duplicated(keep='last', subset=['name'])]
    # この時点で df_temp には名前が同じカードはない

    # 同名カードのレギュレーションを最後のレギュレーションで上書き
    for index, row in df_temp.iterrows():
        df_modified.loc[df_modified['name'] == row['name'], 'regulation'] = row['regulation']

    # 博士の研究を含むカード
    df_temp = df_modified[df_modified['hakase'] == True]
    df_temp = df_temp[~df_temp.duplicated(keep='last', subset=['hakase'])]
    for index, row in df_temp.iterrows():
        df_modified.loc[(df_modified['hakase'] == True), 'regulation'] = row['regulation']

    # ボスの指令を含むカード
    df_temp = df_modified[df_modified['boss'] == True]
    df_temp = df_temp[~df_temp.duplicated(keep='last', subset=['boss'])]
    for index, row in df_temp.iterrows():
        df_modified.loc[(df_modified['boss'] == True), 'regulation'] = row['regulation']

    # 更新
    df.update(df_modified)
    df = df.drop(columns={'hakase','boss'})

    return df

# エキスパンションの名前情報を生成
class expansionFactory:
    expansion_files = "./expansion/*.json"

    def get(self):
        files = glob.glob(self.expansion_files)
        dict_tmp = {}
        counter = 0
        dfExpansion = pd.DataFrame(index=[], columns=['expansion','expansion_name'])
        for file in files:
            with open(file, encoding='utf_8_sig') as f:
                data = json.load(f)
                for item in data['expansion']:
                    dict_tmp[counter] = {'expansion':item['id'], 'expansion_name':item['name']}
                    counter += 1
                dfExpansion = dfExpansion.from_dict(dict_tmp, orient="index")
        return dfExpansion
    
def getExpansionDf(df:pd.DataFrame):
    expa = expansionFactory()
    dfExpa = expa.get()
    #df = pd.merge(df,dfExpa,how='inner',on='expansion')
    df = pd.merge(df,dfExpa,how='outer',on='expansion')
    df['expansion_name'] = df['expansion_name'].fillna('n/a')
    return df

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_ANON_KEY")
service_key: str = os.environ.get("SUPABASE_SERVICE_KEY")
supabase_uri: str = os.environ.get("SUPABASE_URI")

supabase: Client = create_client(url, key)
supabase.postgrest.auth(service_key)

dailyPriceReader = supabaseUtil.CardPriceDailyReader()
editor = supabaseUtil.batchEditor()
writer = supabaseUtil.batchWriter()

currentDT = jst.now()
print(currentDT)

dfCards = loadCardDf()
dfCards = getNewRegulationDf(dfCards)
dfCards = getExpansionDf(dfCards)
print(dfCards)


data_list = []
for index, row in dfCards.iterrows():
    if pd.isnull(row['master_id']):
        print('skip:'+row['name'])
        continue
    record = editor.getCardbase(row)
    data_list.append(record)

for i in range(0, len(data_list), 500):
    batch = data_list[i: i+500]
    print('Write log no.:'+str(i))
    result1 = writer.write(supabase, "card_base", batch)

