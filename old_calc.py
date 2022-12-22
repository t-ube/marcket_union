import glob
import json
import os
import numpy as np
import pandas as pd
from pathlib import Path
import random
import datetime


def getCurrentStock(data):
    if 'price' in data:
        if 'current' in data['price'] and data['price']['current'] is not None:
            current = data['price']['current']
            if 'count' in current and current['count'] is not None:
                return current['count']
    return None

def getWeeklyPriceList(data):
    if 'price' in data:
        if 'weekly' in data['price'] and data['price']['weekly'] is not None:
            weekly = data['price']['weekly']
            if 'archive' in weekly and weekly['archive'] is not None:
                archive = weekly['archive']
                if 'data' in archive and archive['data'] is not None:
                    l = []
                    startPrice = None
                    endPrice = None
                    minPrice = None
                    maxPrice = None
                    for dayData in archive['data']:

                        if startPrice == None:
                            startPrice = dayData['50%']

                        if dayData['50%'] != None:
                            endPrice = dayData['50%']

                        if minPrice == None:
                            minPrice = dayData['50%']
                        else:
                            minPrice = min(minPrice,dayData['50%'])

                        if maxPrice == None:
                            maxPrice = dayData['50%']
                        else:
                            maxPrice = max(maxPrice,dayData['50%'])

                        l.append({
                            'datetime': dayData['datetime'],
                            'stock': dayData['count'],
                            'min': dayData['min'],
                            'p50': dayData['50%']
                        })
                    return {
                        'start': startPrice,
                        'end': endPrice,
                        'min': minPrice,
                        'max': maxPrice,
                        'items': l}
    return None

# tag : 50% or min
def priceWeekly(data,tag):
    if 'price' in data:
        if 'volatility' in data['price'] and data['price']['volatility'] is not None:
            vol = data['price']['volatility']
            if 'weekly' in vol and vol['weekly'] is not None:
                weekly = vol['weekly']
                if tag in weekly and weekly[tag] is not None:
                    mindata = weekly[tag]
                    if 'percent' in mindata and mindata['percent'] is not None:
                        return mindata
    return None

# tag : 50% or min
def priceDaily(data,tag):
    if 'price' in data:
        if 'volatility' in data['price'] and data['price']['volatility'] is not None:
            vol = data['price']['volatility']
            if 'daily' in vol and vol['daily'] is not None:
                daily = vol['daily']
                if tag in daily and daily[tag] is not None:
                    mindata = daily[tag]
                    if 'percent' in mindata and mindata['percent'] is not None:
                        return mindata
    return None

def merge4IncreaseRank(dfList,dfExp):
    dfList = dfList.replace([np.inf, -np.inf], 0.0)
    dfList = dfList.fillna('n/a')
    dfList = pd.merge(dfList,dfExp,how='inner',on='expansion')
    dfList['rank'] = dfList.rank(ascending=False)['percent_24h']
    dfList = dfList.sort_values(by=['rank'], ascending=[True])
    '''
    dfList = dfList.replace([np.inf, -np.inf], 0.0)
    dfList['rank'] = dfList.rank(ascending=False)['percent_24h']
    dfList = dfList.fillna('n/a')
    dfList = pd.merge(dfList,dfExp,how='inner',on='expansion')
    '''
    return dfList

def merge4DecreaseRank(dfList,dfExp):
    dfList = dfList.replace([np.inf, -np.inf], 0.0)
    dfList = dfList.fillna('n/a')
    dfList = pd.merge(dfList,dfExp,how='inner',on='expansion')
    dfList['rank'] = dfList.rank(ascending=True)['percent_24h']
    dfList = dfList.sort_values(by=['rank'], ascending=[True])
    '''
    dfList = dfList.replace([np.inf, -np.inf], 0.0)
    dfList['rank'] = dfList.rank(ascending=True)['percent_24h']
    dfList = dfList.fillna('n/a')
    dfList = pd.merge(dfList,dfExp,how='inner',on='expansion')
    '''
    return dfList

def merge4NewExpansionRank(dfList,dfExp,fillterExp):
    dfList['rank'] = 0.0
    dfList = dfList.replace([np.inf, -np.inf], 0.0)
    dfList = dfList.fillna('n/a')
    dfList = pd.merge(dfList,dfExp,how='inner',on='expansion')
    dfList['rank'] = dfList.rank(ascending=False)['latest_price']
    dfList = dfList.sort_values(by=['rank'], ascending=[True])
    '''
    dfList['rank'] = 0.0
    dfList = pd.merge(dfList,dfExp,how='inner',on='expansion')
    dfList = dfList[dfList['expansion'] == fillterExp]
    dfList = dfList.replace([np.inf, -np.inf], 0.0)
    dfList['rank'] = dfList.rank(ascending=False)['latest_price']
    dfList = dfList.fillna('n/a')
    '''
    return dfList
    
def merge4PriceRank(dfList,dfExp):
    dfList = dfList.replace([np.inf, -np.inf], np.nan)
    dfList = dfList.fillna('n/a')
    dfList = pd.merge(dfList,dfExp,how='inner',on='expansion')
    dfList['rank'] = dfList.rank(ascending=False)['latest_price']
    return dfList

def saveDfItems(df,_file):
    '''
    df.to_json(path_or_buf=_file,
        force_ascii=False,
        orient='records',
        lines=True)
    '''
    dict_tmp = {'items': []}
    dict_tmp['items'] = df.to_dict(orient="record")
    with open(_file, 'w', encoding='utf_8_sig') as f:
        json.dump(dict_tmp, f, ensure_ascii=False)

def saveDict(dict,_file):
    with open(_file, 'w', encoding='utf_8_sig') as f:
        json.dump(dict, f, ensure_ascii=False)

# sub_type   : サポート or グッズ or スタジアム or ポケモンのどうぐ
# _data_type : latest_price or percent_7d
def merge4TrainersPriceRank(dfList,dfExp,sub_type,_data_type):
    dfList = dfList.replace([np.inf, -np.inf], 0.0)
    dfList = dfList.fillna('n/a')
    dfList = pd.merge(dfList,dfExp,how='inner',on='expansion')
    dfList = dfList[((dfList['card_type'] == 'トレーナーズ') & (dfList['sub_type'] == sub_type) &
     ((dfList['rarity'] == 'C') | (dfList['rarity'] == 'U') | (dfList['rarity'] == 'n/a') | (dfList['rarity'] == '-') ) &
      (dfList['expansion_name'] != 'プロモカード') &
      ((dfList['regulation'] == 'D') | (dfList['regulation'] == 'E') | (dfList['regulation'] == 'F')))]
    dfList = dfList.sort_values(by=['stock'], ascending=[True])
    dfList = dfList[~dfList.duplicated(keep='last', subset='name')]
    dfList['rank'] = dfList.rank(ascending=False)[_data_type]
    dfList = dfList.sort_values(by=['rank'], ascending=[True])
    return dfList


# _data_type : latest_price or percent_7d
def merge4PokePriceRank(dfList,dfExp,_data_type):
    dfList = dfList.replace([np.inf, -np.inf], 0.0)
    dfList = dfList.fillna('n/a')
    dfList = pd.merge(dfList,dfExp,how='inner',on='expansion')
    dfList = dfList[((dfList['card_type'] == 'ポケモン') &
     ((dfList['rarity'] == 'C') | (dfList['rarity'] == 'U') | (dfList['rarity'] == 'R') | (dfList['rarity'] == 'RR') | (dfList['rarity'] == 'RRR') | (dfList['rarity'] == 'A') | (dfList['rarity'] == 'K')) &
      (dfList['expansion_name'] != 'プロモカード') &
      ((dfList['regulation'] == 'D') | (dfList['regulation'] == 'E') | (dfList['regulation'] == 'F')))]
    dfList = dfList.sort_values(by=['stock'], ascending=[True])
    dfList = dfList[~dfList.duplicated(keep='last', subset='name')]
    dfList['rank'] = dfList.rank(ascending=False)[_data_type]
    dfList = dfList.sort_values(by=['rank'], ascending=[True])
    return dfList


# sub_type : サポート or グッズ or スタジアム or ポケモンのどうぐ
def merge4TrainersPriceRankHighRare(dfList,dfExp,sub_type):
    dfList = dfList.replace([np.inf, -np.inf], np.nan)
    dfList = dfList.fillna('n/a')
    dfList = pd.merge(dfList,dfExp,how='inner',on='expansion')
    dfList = dfList[((dfList['card_type'] == 'トレーナーズ') & (dfList['sub_type'] == sub_type) &
     (((dfList['rarity'] != 'C') & (dfList['rarity'] != 'U')) | 
      (dfList['expansion_name'] == 'プロモカード')))]
    dfList = dfList.sort_values(by=['stock'], ascending=[True])
    dfList = dfList[~dfList.duplicated(keep='last', subset='name')]
    dfList['rank'] = dfList.rank(ascending=False)['latest_price']
    dfList = dfList.sort_values(by=['rank'], ascending=[True])
    return dfList

def merge4SupportSRPriceRankHighRare(dfList,dfExp):
    dfList = dfList.replace([np.inf, -np.inf], np.nan)
    dfList = dfList.fillna('n/a')
    dfList = pd.merge(dfList,dfExp,how='inner',on='expansion')
    dfList = dfList[((dfList['card_type'] == 'トレーナーズ') & (dfList['sub_type'] == 'サポート') &
     (dfList['rarity'] == 'SR'))]
    dfList = dfList.sort_values(by=['stock'], ascending=[True])
    dfList = dfList[~dfList.duplicated(keep='last', subset='name')]
    dfList['rank'] = dfList.rank(ascending=False)['latest_price']
    dfList = dfList.sort_values(by=['rank'], ascending=[True])
    return dfList

def merge4PokePriceRankHighRare(dfList,dfExp):
    dfList = dfList.replace([np.inf, -np.inf], np.nan)
    dfList = dfList.fillna('n/a')
    dfList = pd.merge(dfList,dfExp,how='inner',on='expansion')
    dfList = dfList[((dfList['card_type'] == 'ポケモン') &
     (((dfList['rarity'] != 'C') & (dfList['rarity'] != 'U') & (dfList['rarity'] != 'R') & (dfList['rarity'] != 'RR') & (dfList['rarity'] != 'RRR') & (dfList['rarity'] != 'A') & (dfList['rarity'] != 'K')) | 
      (dfList['expansion_name'] == 'プロモカード')))]
    dfList['rank'] = dfList.rank(ascending=False)['latest_price']
    dfList = dfList.sort_values(by=['rank'], ascending=[True])
    return dfList

def merge4EeveePriceRankHighRare(dfList,dfExp):
    dfList = dfList.replace([np.inf, -np.inf], np.nan)
    dfList = dfList.fillna('n/a')
    dfList = pd.merge(dfList,dfExp,how='inner',on='expansion')
    dfList = dfList[((dfList['card_type'] == 'ポケモン') &
     (((dfList['rarity'] != 'C') & (dfList['rarity'] != 'U') & (dfList['rarity'] != 'R') & (dfList['rarity'] != 'RR') & (dfList['rarity'] != 'RRR') & (dfList['rarity'] != 'A') & (dfList['rarity'] != 'K')) | 
      (dfList['expansion_name'] == 'プロモカード')))]
    dfList = dfList[(dfList['name'].str.contains('イーブイ')) | 
        (dfList['name'].str.contains('ブースター')) | 
        (dfList['name'].str.contains('シャワーズ')) |
        (dfList['name'].str.contains('サンダース')) |
        (dfList['name'].str.contains('ブラッキー')) |
        (dfList['name'].str.contains('エーフィー')) |
        (dfList['name'].str.contains('ニンフィア')) |
        (dfList['name'].str.contains('グレイシア')) |
        (dfList['name'].str.contains('リーフィア'))]
    dfList['rank'] = dfList.rank(ascending=False)['latest_price']
    dfList = dfList.sort_values(by=['rank'], ascending=[True])
    return dfList

def merge4CharizardPriceRankHighRare(dfList,dfExp):
    dfList = dfList.replace([np.inf, -np.inf], np.nan)
    dfList = dfList.fillna('n/a')
    dfList = pd.merge(dfList,dfExp,how='inner',on='expansion')
    dfList = dfList[((dfList['card_type'] == 'ポケモン') &
     (((dfList['rarity'] != 'C') & (dfList['rarity'] != 'U') & (dfList['rarity'] != 'R') & (dfList['rarity'] != 'RR') & (dfList['rarity'] != 'RRR') & (dfList['rarity'] != 'A') & (dfList['rarity'] != 'K')) | 
      (dfList['expansion_name'] == 'プロモカード')))]
    dfList = dfList[(dfList['name'].str.contains('リザードン'))]
    dfList['rank'] = dfList.rank(ascending=False)['latest_price']
    dfList = dfList.sort_values(by=['rank'], ascending=[True])
    return dfList

def merge4PikachuPriceRankHighRare(dfList,dfExp):
    dfList = dfList.replace([np.inf, -np.inf], np.nan)
    dfList = dfList.fillna('n/a')
    dfList = pd.merge(dfList,dfExp,how='inner',on='expansion')
    dfList = dfList[((dfList['card_type'] == 'ポケモン') &
     (((dfList['rarity'] != 'C') & (dfList['rarity'] != 'U') & (dfList['rarity'] != 'R') & (dfList['rarity'] != 'RR') & (dfList['rarity'] != 'RRR') & (dfList['rarity'] != 'A') & (dfList['rarity'] != 'K')) | 
      (dfList['expansion_name'] == 'プロモカード')))]
    dfList = dfList[(dfList['name'].str.contains('ピカチュウ'))]
    dfList['rank'] = dfList.rank(ascending=False)['latest_price']
    dfList = dfList.sort_values(by=['rank'], ascending=[True])
    return dfList

def merge4CHRPriceRankHighRare(dfList,dfExp):
    dfList = dfList.replace([np.inf, -np.inf], np.nan)
    dfList = dfList.fillna('n/a')
    dfList = pd.merge(dfList,dfExp,how='inner',on='expansion')
    dfList = dfList[((dfList['card_type'] == 'ポケモン') & (dfList['rarity'] == 'CHR'))]
    dfList['rank'] = dfList.rank(ascending=False)['latest_price']
    dfList = dfList.sort_values(by=['rank'], ascending=[True])
    return dfList

def merge4PriceRankHighRare(dfList,dfExp):
    dfList = dfList.replace([np.inf, -np.inf], np.nan)
    dfList = dfList.fillna('n/a')
    dfList = pd.merge(dfList,dfExp,how='inner',on='expansion')
    dfList = dfList[(
     ((dfList['rarity'] != 'C') & (dfList['rarity'] != 'U') & (dfList['rarity'] != 'R') & (dfList['rarity'] != 'RR') & (dfList['rarity'] != 'RRR') & (dfList['rarity'] != 'A') & (dfList['rarity'] != 'K')) | 
      (dfList['expansion_name'] == 'プロモカード'))]
    dfList['rank'] = dfList.rank(ascending=False)['latest_price']
    dfList = dfList.sort_values(by=['rank'], ascending=[True])
    return dfList

def get7daysTop5Price(dfPrice):
    dfPriceTop5 = dfPrice.head(10)

    ranking = list()
    for id, row in dfPriceTop5.iterrows():
        ranking.append({
            'master_id' : row['master_id'],
            'latest_price': row['latest_price'],
            'price_24h': row['price_24h'],
            'diff_24h': row['diff_24h'],
            'percent_24h': row['percent_24h'],
            'price_7d': row['price_7d'],
            'diff_7d': row['diff_7d'],
            'percent_7d': row['percent_7d'],
            'stock': row['stock'],
            'rank': row['rank'],
            'name': row['name'],
            'expansion': row['expansion'],
            'expansion_name': row['expansion_name'],
            'rarity': row['rarity'],
            'cn': row['cn'],
            'card_type': row['card_type'],
            'sub_type': row['sub_type'],
        })

    legend = list()
    index = 1
    for id, row in dfPriceTop5.iterrows():
        legend.append({
            'master_id' : row['master_id'],
            'rank': row['rank'],
            'name': row['name'],
            'expansion': row['expansion'],
            'expansion_name': row['expansion_name'],
            'rarity': row['rarity'],
            'cn': row['cn'],
            'card_type': row['card_type'],
            'sub_type': row['sub_type'],
            'color': "#"+''.join([random.choice('0123456789ABCDEF') for j in range(6)]),
            'key': 'data' + str(index)
        })
        index += 1

    records = list()
    for i in range(7):
        records.append({
            'date' : None,
            'data1': None,
            'data2': None,
            'data3': None,
            'data4': None,
            'data5': None,
            'data6': None,
            'data7': None,
            'data8': None,
            'data9': None,
            'data10': None,
        })

    index = 1
    name = 'data0'
    for cname, item in dfPriceTop5['price_list_7d'].iteritems():
        name = 'data'+str(index)
        for i, dateData in enumerate(item['items']):
            dt = datetime.datetime.strptime(dateData['datetime'], "%Y-%m-%d %H:%M:%S")
            records[i]['date'] = dt.strftime("%Y-%m-%d")
            records[i][name] = dateData['p50']
        index += 1
    
    dict_tmp = {
        'ranking' : ranking,
        'chart_line' : {'legend': legend, 'items': records}
    }
    return dict_tmp

Path('./dist').mkdir(parents=True, exist_ok=True)

listWeekly = {}
listDaily = {}
listStock = {}
listWeeklyPriceList = {}
files = glob.glob("./marcket/*.json")
for file in files:
    with open(file, encoding='utf_8_sig') as f:
        masterid = os.path.splitext(os.path.basename(file))[0]
        data = json.load(f)
        priceW = priceWeekly(data,'50%')
        priceW_Min = priceWeekly(data,'min')
        if priceW is not None and priceW_Min is not None:
            listWeekly[len(listWeekly)] = {
                'master_id': masterid,
                'base_price': priceW['basePrice'],
                'latest_price': priceW['latestPrice'],
                'percent': priceW['percent'],
                'min_price': priceW_Min['latestPrice'],
            }
        priceD = priceDaily(data,'50%')
        priceD_Min = priceDaily(data,'min')
        if priceD is not None and priceD_Min is not None:
            listDaily[len(listDaily)] = {
                'master_id': masterid,
                'base_price': priceD['basePrice'],
                'latest_price': priceD['latestPrice'],
                'percent': priceD['percent'],
                'min_price': priceD_Min['latestPrice'],
            }
        stock = getCurrentStock(data)
        if stock is not None:
            listStock[len(listStock)] = {
                'master_id': masterid,
                'stock': stock,
            }
        weeklyPriceList = getWeeklyPriceList(data)
        if weeklyPriceList is not None:
            listWeeklyPriceList[len(listWeeklyPriceList)] = {
                'master_id': masterid,
                'price_list_7d': weeklyPriceList,
            }

dfDaily = pd.DataFrame.from_dict(listDaily, orient='index')
dfWeekly = pd.DataFrame.from_dict(listWeekly, orient='index')
dfStock = pd.DataFrame.from_dict(listStock, orient='index')
dfWeeklyPriceList = pd.DataFrame.from_dict(listWeeklyPriceList, orient='index')

dfDaily = pd.merge(dfDaily,dfWeekly,how='inner',on='master_id')
dfDaily = pd.merge(dfDaily,dfStock,how='inner',on='master_id')
dfDaily = pd.merge(dfDaily,dfWeeklyPriceList,how='inner',on='master_id')
dfDaily = dfDaily.drop(columns={'latest_price_y'})
dfDaily = dfDaily.drop(columns={'min_price_y'})
dfDaily = dfDaily.rename(columns={
    'latest_price_x': 'latest_price',
    'percent_x': 'percent_24h',
    'percent_y': 'percent_7d',
    'base_price_x': 'price_24h',
    'base_price_y': 'price_7d',
    'min_price_x': 'min_price',
    })
dfDaily['diff_24h'] = dfDaily['latest_price'] - dfDaily['price_24h']
dfDaily['diff_7d'] = dfDaily['latest_price'] - dfDaily['price_7d']

files = glob.glob("./card/*.csv")
for file in files:
    readDf = pd.read_csv(
        file,
        encoding="utf_8_sig", sep=",",
        header=0)
    dfDaily = pd.merge(dfDaily,readDf,how='inner',on='master_id')

files = glob.glob("./expansion/*.json")
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

dfPrice = merge4PriceRank(dfDaily,dfExpansion)
dfPrice['rarity'] = dfPrice['rarity'].str.replace('-','n/a')

dfPrice = dfPrice.replace([np.inf, -np.inf], 0.0)
dfPrice.loc[dfPrice.percent_24h == 'n/a','percent_24h']=0
dfPrice.loc[dfPrice.percent_7d == 'n/a','percent_7d']=0
dfPrice.loc[dfPrice.is_mirror == 'n/a','is_mirror']=False
dfPrice = dfPrice.fillna({'percent_24h': 0, 'percent_7d': 0})
dfPrice = dfPrice.fillna('n/a')
dfPrice = dfPrice.sort_values(by=['rank'], ascending=[True])
saveDfItems(dfPrice,'./dist/daily_price_cardlist.json')
dfPriceTop100 = dfPrice.head(100)
saveDfItems(dfPriceTop100,'./dist/daily_price_cardlist_top100.json')

# ダッシュボード用のデータを構築
summaryRare = {
    'all': get7daysTop5Price(merge4PriceRankHighRare(dfDaily,dfExpansion)),
    'poke': get7daysTop5Price(merge4PokePriceRankHighRare(dfDaily,dfExpansion)),
    'support': get7daysTop5Price(merge4TrainersPriceRankHighRare(dfDaily,dfExpansion,'サポート')),
    'goods': get7daysTop5Price(merge4TrainersPriceRankHighRare(dfDaily,dfExpansion,'グッズ')),
    'tools': get7daysTop5Price(merge4TrainersPriceRankHighRare(dfDaily,dfExpansion,'ポケモンのどうぐ')),
    'stadium': get7daysTop5Price(merge4TrainersPriceRankHighRare(dfDaily,dfExpansion,'スタジアム')),
    'eevee': get7daysTop5Price(merge4EeveePriceRankHighRare(dfDaily,dfExpansion)),
    'charizard': get7daysTop5Price(merge4CharizardPriceRankHighRare(dfDaily,dfExpansion)),
    'chr': get7daysTop5Price(merge4CHRPriceRankHighRare(dfDaily,dfExpansion)),
    'pikachu': get7daysTop5Price(merge4PikachuPriceRankHighRare(dfDaily,dfExpansion)),
    'support_sr': get7daysTop5Price(merge4SupportSRPriceRankHighRare(dfDaily,dfExpansion)),
}
saveDict(summaryRare,'./dist/summary_card_rare_top5.json')

summaryBattle = {
    'poke': {
        'price': get7daysTop5Price(merge4PokePriceRank(dfDaily,dfExpansion,'latest_price')),
        'diff_7d': get7daysTop5Price(merge4PokePriceRank(dfDaily,dfExpansion,'diff_7d')),
        'increase_7d': get7daysTop5Price(merge4PokePriceRank(dfDaily,dfExpansion,'percent_7d'))
    },
    'support': {
        'price': get7daysTop5Price(merge4TrainersPriceRank(dfDaily,dfExpansion,'サポート','latest_price')),
        'diff_7d': get7daysTop5Price(merge4TrainersPriceRank(dfDaily,dfExpansion,'サポート','diff_7d')),
        'increase_7d': get7daysTop5Price(merge4TrainersPriceRank(dfDaily,dfExpansion,'サポート','percent_7d'))
    },
    'goods': {
        'price': get7daysTop5Price(merge4TrainersPriceRank(dfDaily,dfExpansion,'グッズ','latest_price')),
        'diff_7d': get7daysTop5Price(merge4TrainersPriceRank(dfDaily,dfExpansion,'グッズ','diff_7d')),
        'increase_7d': get7daysTop5Price(merge4TrainersPriceRank(dfDaily,dfExpansion,'グッズ','percent_7d'))
    },
    'tools': {
        'price': get7daysTop5Price(merge4TrainersPriceRank(dfDaily,dfExpansion,'ポケモンのどうぐ','latest_price')),
        'diff_7d': get7daysTop5Price(merge4TrainersPriceRank(dfDaily,dfExpansion,'ポケモンのどうぐ','diff_7d')),
        'increase_7d': get7daysTop5Price(merge4TrainersPriceRank(dfDaily,dfExpansion,'ポケモンのどうぐ','percent_7d'))
    },
    'stadium': {
        'price': get7daysTop5Price(merge4TrainersPriceRank(dfDaily,dfExpansion,'スタジアム','latest_price')),
        'diff_7d': get7daysTop5Price(merge4TrainersPriceRank(dfDaily,dfExpansion,'スタジアム','diff_7d')),
        'increase_7d': get7daysTop5Price(merge4TrainersPriceRank(dfDaily,dfExpansion,'スタジアム','percent_7d'))
    },
}
saveDict(summaryBattle,'./dist/summary_card_battle_top5.json')

dfReguStock = dfPrice[['regulation', 'stock']].groupby('regulation').sum()
dfReguStock = dfReguStock.sort_values(by=['stock'], ascending=[False])
dfReguStock.reset_index(inplace= True)
dfReguStock = dfReguStock.rename(columns={'regulation':'name'}).rename(columns={'stock':'value'})
saveDfItems(dfReguStock,'./dist/daily_regu_stock_cardlist.json')

dfRarityStock = dfPrice[['rarity', 'stock']].groupby('rarity').sum()
dfRarityStock = dfRarityStock.sort_values(by=['stock'], ascending=[False])
dfRarityStock.reset_index(inplace= True)
dfRarityStock = dfRarityStock.rename(columns={'rarity':'name'}).rename(columns={'stock':'value'})
saveDfItems(dfRarityStock,'./dist/daily_rarity_stock_cardlist.json')

summaryGeneral = {
    'increase': get7daysTop5Price(merge4IncreaseRank(dfDaily,dfExpansion)),
    'decrease': get7daysTop5Price(merge4DecreaseRank(dfDaily,dfExpansion)),
    'new_expansion': get7daysTop5Price(merge4NewExpansionRank(dfDaily,dfExpansion,'S12')),
}
saveDict(summaryGeneral,'./dist/summary_card_general_top5.json')

'''
dfDaily1 = dfDaily.sort_values(by=['percent','latest_price'], ascending=[False,False])
dfDaily1 = merge4VolaRank(dfDaily1,dfExpansion)
saveDfItems(dfDaily1,'./dist/daily_vol_cardlist.json')
print(dfDaily1)

dfWeekly3 = dfWeekly.sort_values(by=['latest_price'], ascending=[False])
dfWeekly3 = merge4PriceRank(dfWeekly3,dfExpansion)
saveDfItems(dfWeekly3,'./dist/weekly_price_cardlist.json')
print(dfWeekly3)

dfWeekly2 = dfWeekly[['expansion', 'latest_price']].groupby('expansion').sum().sort_values(by=['latest_price'], ascending=[False])
dfWeekly2 = merge4PriceRank(dfWeekly2, dfExpansion)
saveDfItems(dfWeekly2,'./dist/weekly_price_explist.json')
print(dfWeekly2)

dfDaily2 = dfDaily[['expansion', 'latest_price']].groupby('expansion').sum().sort_values(by=['latest_price'], ascending=[False])
dfDaily2 = merge4PriceRank(dfDaily2, dfExpansion)
saveDfItems(dfDaily2,'./dist/daily_price_explist.json')
print(dfDaily2)
'''