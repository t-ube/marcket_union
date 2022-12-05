import json
import glob
import pandas as pd
import os
import numpy as np
from pathlib import Path
import random
import datetime

# シティリーグ使用されたIDを表示
class cityLeagueLister:
    def getList(self):
        array_id_4w = []
        with open("event/cl.json", "r") as f:
            data = json.load(f)

        if 'items' in data:
            for item in data['items']:
                if 'index' in item:
                    if item['index'] >= 0 and item['index'] < 4:
                        if 'card_list' in item:
                            for card in item['card_list']:
                                if 'card_id' in card:
                                    array_id_4w.append(card['card_id'])
        array_id_4w = list(set(array_id_4w))
        return array_id_4w

# シティリーグで使用されたデッキの分類をカウントする
class cityLeagueDeckCounter:
    def get(self):
        array = []
        with open("event/cl.json", "r") as f:
            data = json.load(f)

        if 'items' in data:
            for item in data['items']:
                if 'index' in item:
                    if item['index'] == 0:
                        if 'deck_type' in item:
                            keys = item['deck_type'].keys()
                            for key in keys:
                                deck_item = item['deck_type'][key]
                                array.append({
                                    'name': key,
                                    'value': deck_item['count']
                                })
        array.sort(key=lambda x: x['value'], reverse=True)
        df = pd.DataFrame(array)
        return df

    def getRank1(self):
        array = []
        with open("event/cl.json", "r") as f:
            data = json.load(f)

        if 'items' in data:
            for item in data['items']:
                if 'index' in item:
                    if item['index'] == 0:
                        if 'deck_type_rank1' in item:
                            keys = item['deck_type_rank1'].keys()
                            for key in keys:
                                deck_item = item['deck_type_rank1'][key]
                                array.append({
                                    'name': key,
                                    'value': deck_item['count']
                                })
        array.sort(key=lambda x: x['value'], reverse=True)
        df = pd.DataFrame(array)
        return df

    def _getRanking(self, df: pd.DataFrame):
        df['rank'] = df.rank(ascending=False)['value']
        df = df.sort_values(by=['rank'], ascending=[True])
        topDf = df.head(10)
        ranking = list()
        for id, row in topDf.iterrows():
            ranking.append({
                'rank' : row['rank'],
                'name' : row['name'],
                'value': row['value'],
            })
        return ranking

    def save(self, df: pd.DataFrame, file_name: str):
        dict_tmp = {'ranking':[],'items': []}
        dict_tmp['ranking'] = self._getRanking(df)
        dict_tmp['items'] = df.to_dict(orient="record")
        with open(file_name, 'w', encoding='utf_8_sig') as f:
            json.dump(dict_tmp, f, ensure_ascii=False)


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

# 価格情報の基本情報を生成
class dailyPriceFactory:
    marcket_files = "./marcket/*.json"

    def get(self):
        listWeekly = {}
        listDaily = {}
        listStock = {}
        listWeeklyPriceList = {}
        files = glob.glob(self.marcket_files)
        for file in files:
            with open(file, encoding='utf_8_sig') as f:
                masterid = os.path.splitext(os.path.basename(file))[0]
                data = json.load(f)
                priceW = self.priceWeekly(data,'50%')
                priceW_Min = self.priceWeekly(data,'min')
                if priceW is not None and priceW_Min is not None:
                    listWeekly[len(listWeekly)] = {
                        'master_id': masterid,
                        'base_price': priceW['basePrice'],
                        'latest_price': priceW['latestPrice'],
                        'percent': priceW['percent'],
                        'min_price': priceW_Min['latestPrice'],
                    }
                priceD = self.priceDaily(data,'50%')
                priceD_Min = self.priceDaily(data,'min')
                if priceD is not None and priceD_Min is not None:
                    listDaily[len(listDaily)] = {
                        'master_id': masterid,
                        'base_price': priceD['basePrice'],
                        'latest_price': priceD['latestPrice'],
                        'percent': priceD['percent'],
                        'min_price': priceD_Min['latestPrice'],
                    }
                stock = self.getCurrentStock(data)
                if stock is not None:
                    listStock[len(listStock)] = {
                        'master_id': masterid,
                        'stock': stock,
                    }
                weeklyPriceList = self.getWeeklyPriceList(data)
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

        return dfDaily


    # tag : 50% or min
    def priceWeekly(self,data,tag):
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
    def priceDaily(self,data,tag):
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

    def getCurrentStock(self,data):
        if 'price' in data:
            if 'current' in data['price'] and data['price']['current'] is not None:
                current = data['price']['current']
                if 'count' in current and current['count'] is not None:
                    return current['count']
        return None

    def getWeeklyPriceList(self,data):
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

# ランク集計
class rankCalculator:
    is_filtered_dupcard = False
    is_filtered_regulation = False
    is_rank_invert = False
    filtered_expansion = None
    filtered_id_list = None
    rank_price_type = 'latest_price' # rank_price_type : latest_price or percent_7d or diff_7d or percent_24h

    def _merge(self, dfList: pd.DataFrame, dfExp : pd.DataFrame):
        dfList['rank'] = 0.0
        dfList = dfList.replace([np.inf, -np.inf], 0.0)
        dfList = dfList.fillna('n/a')
        dfList = pd.merge(dfList,dfExp,how='inner',on='expansion')
        return dfList

    def _filterRegulation(self, df: pd.DataFrame):
        if self.is_filtered_regulation:
            df = df[((df['regulation'] == 'D') | (df['regulation'] == 'E') | (df['regulation'] == 'F'))]
        return df

    def _filterDuplicatedStock(self, dfList: pd.DataFrame):
        if self.is_filtered_dupcard:
            dfList = dfList.sort_values(by=['stock'], ascending=[True])
            dfList = dfList[~dfList.duplicated(keep='last', subset=['name','ability','move1','move2'])]
        return dfList

    def _filterDuplicatedRank(self, dfList: pd.DataFrame):
        if self.is_filtered_dupcard:
            dfList = dfList.sort_values(by=['rank'], ascending=[False])
            dfList = dfList[~dfList.duplicated(keep='last', subset=['name','ability','move1','move2'])]
        return dfList

    def _filterExpansion(self, dfList: pd.DataFrame):
        if self.filtered_expansion != None:
            dfList = dfList[dfList['expansion'] == self.filtered_expansion]
        return dfList

    def _filterIDList(self, dfList: pd.DataFrame):
        if self.filtered_id_list != None:
            result = dfList['official_id'].apply(lambda x: any(char in x for char in self.filtered_id_list))
            dfList = dfList[result]
        return dfList
    
    def _addRank(self, dfList: pd.DataFrame):
        if self.is_rank_invert:
            dfList['rank'] = dfList.rank(ascending=True)[self.rank_price_type]
        else:
            dfList['rank'] = dfList.rank(ascending=False)[self.rank_price_type]
        return dfList

    def setRankType(self, type: str):
        self.rank_price_type = type

    def getPriceRank(self, dfList: pd.DataFrame, dfExp: pd.DataFrame):
        dfPrice = self._merge(dfList, dfExp)
        dfPrice = self._filterIDList(dfPrice)
        dfPrice = self._filterExpansion(dfPrice)
        dfPrice = self._filterRegulation(dfPrice)
        dfPrice = self._addRank(dfPrice)

        dfPrice['rarity'] = dfPrice['rarity'].str.replace('-','n/a')
        dfPrice = dfPrice.replace([np.inf, -np.inf], 0.0)
        dfPrice.loc[dfPrice.percent_24h == 'n/a','percent_24h']=0
        dfPrice.loc[dfPrice.percent_7d == 'n/a','percent_7d']=0
        dfPrice.loc[dfPrice.is_mirror == 'n/a','is_mirror']=False
        dfPrice = dfPrice.fillna({'percent_24h': 0, 'percent_7d': 0})
        dfPrice = dfPrice.fillna('n/a')
        dfPrice = self._filterDuplicatedRank(dfPrice)
        dfPrice = dfPrice.sort_values(by=['rank'], ascending=[True])
        return dfPrice

    # sub_type : サポート or グッズ or スタジアム or ポケモンのどうぐ
    def filterTrainers(self, df: pd.DataFrame, sub_type: str):
        df = df[((df['card_type'] == 'トレーナーズ') & (df['sub_type'] == sub_type))]
        return df

    def save(self, df: pd.DataFrame, file_name: str):
        dict_tmp = {'items': []}
        dict_tmp['items'] = df.to_dict(orient="record")
        with open(file_name, 'w', encoding='utf_8_sig') as f:
            json.dump(dict_tmp, f, ensure_ascii=False)

class calcTopPrice:
    def get7daysTopPrice(self, df: pd.DataFrame):
        dfPriceTops = df.head(10)
        ranking = list()
        for id, row in dfPriceTops.iterrows():
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
        for id, row in dfPriceTops.iterrows():
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
        for cname, item in dfPriceTops['price_list_7d'].iteritems():
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

    def save(self, dict: dict, file_name: str):
        with open(file_name, 'w', encoding='utf_8_sig') as f:
            json.dump(dict, f, ensure_ascii=False)

# 集計監査
class auditStockLogger:
    # df : priceCalculator:get で出力した df
    def saveRegulation(self, df: pd.DataFrame, file_name: str):
        dfStock = df[['regulation', 'stock']].groupby('regulation').sum()
        dfStock = dfStock.sort_values(by=['stock'], ascending=[False])
        dfStock.reset_index(inplace= True)
        dfStock = dfStock.rename(columns={'regulation':'name'}).rename(columns={'stock':'value'})
        self._save(dfStock, file_name)

    def saveRarity(self, df: pd.DataFrame, file_name: str):
        dfStock = df[['rarity', 'stock']].groupby('rarity').sum()
        dfStock = dfStock.sort_values(by=['stock'], ascending=[False])
        dfStock.reset_index(inplace= True)
        dfStock = dfStock.rename(columns={'rarity':'name'}).rename(columns={'stock':'value'})
        self._save(dfStock, file_name)

    def _save(self, df: pd.DataFrame, file_name: str):
        dict_tmp = {'items': []}
        dict_tmp['items'] = df.to_dict(orient="record")
        with open(file_name, 'w', encoding='utf_8_sig') as f:
            json.dump(dict_tmp, f, ensure_ascii=False)

output_dir = './dist'
Path(output_dir).mkdir(parents=True, exist_ok=True)

exp_fact = expansionFactory()
daily_fact = dailyPriceFactory()
expDf = exp_fact.get()
dailyDf = daily_fact.get()

# 全カード対象
topCalc = calcTopPrice()
ranks = rankCalculator()
priceDf = ranks.getPriceRank(dailyDf,expDf)
ranks.save(priceDf,output_dir+'/all.json')
ranks.save(priceDf.head(50),output_dir+'/all_head.json')

audit = auditStockLogger()
audit.saveRegulation(priceDf, output_dir+'/all_stock_regu.json')
audit.saveRarity(priceDf, output_dir+'/all_stock_rarity.json')

ranks.is_filtered_dupcard = True
priceDf = ranks.getPriceRank(dailyDf,expDf)
topDf = topCalc.get7daysTopPrice(priceDf)
topCalc.save(topDf, output_dir+'/all_price_top.json')

ranks.rank_price_type = 'percent_24h'
ranks.is_filtered_dupcard = True
priceDf = ranks.getPriceRank(dailyDf,expDf)
topDf = topCalc.get7daysTopPrice(priceDf)
topCalc.save(topDf, output_dir+'/all_price_rise_24h_top.json')


# シティリーグ対象
topCalc = calcTopPrice()
ranksCL = rankCalculator()
ranksCL.filtered_id_list = cityLeagueLister().getList()
priceDf = ranksCL.getPriceRank(dailyDf,expDf)
ranksCL.save(priceDf,output_dir+'/cl.json')
ranksCL.save(priceDf.head(50),output_dir+'/cl_head.json')
#ranksCL.rank_price_type = 'diff_7d'
ranksCL.rank_price_type = 'percent_7d'
ranksCL.is_filtered_dupcard = True
priceDf = ranksCL.getPriceRank(dailyDf,expDf)
topDf = topCalc.get7daysTopPrice(priceDf)
topCalc.save(topDf, output_dir+'/cl_price_rise_top.json')
ranksCL.is_rank_invert = True
priceDf = ranksCL.getPriceRank(dailyDf,expDf)
topDf = topCalc.get7daysTopPrice(priceDf)
topCalc.save(topDf, output_dir+'/cl_price_fall_top.json')

counterCL = cityLeagueDeckCounter()
counterDf = counterCL.get()
counterCL.save(counterDf,output_dir+'/cl_deck_top.json')
counterDf = counterCL.getRank1()
counterCL.save(counterDf,output_dir+'/cl_deck_rank1_top.json')

# 新弾対象
topCalc = calcTopPrice()
ranksNew = rankCalculator()
ranksNew.filtered_expansion = 'S12'
priceDf = ranksNew.getPriceRank(dailyDf,expDf)
topDf = topCalc.get7daysTopPrice(priceDf)
topCalc.save(topDf, output_dir+'/new_product_price_top.json')
