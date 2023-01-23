import json
import glob
import pandas as pd
import os
import numpy as np
from pathlib import Path
import random
import datetime
import gc
import httpx
import postgrest
import urllib.request
import math
from supabase import create_client, Client

# supabase の読み取り
class cardSummaryIndexReader:
    def read(self, supabase:Client):
        try:
            data = supabase.table("card_market_result_index").select("master_id_list").execute()
            if len(data.data) == 0:
                return []
            if data.data[0]['master_id_list'] == None:
                return []
            return data.data[0]['master_id_list'].split(',')
        except httpx.ReadTimeout as e:
            print("httpx.ReadTimeout")
            print(e.args)
        except postgrest.exceptions.APIError as e:
            print("postgrest.exceptions.APIError")
            print(e.args)
        return []

# supabase の読み取り
class cardSummaryReader:
    def read(self, supabase:Client, id_list):
        try:
            data = supabase.table("card_market_result").select("master_id,card_price").in_("master_id",id_list).execute()
            if len(data.data) == 0:
                return []
            return data.data
        except httpx.ReadTimeout as e:
            print("httpx.ReadTimeout")
            print(e.args)
        except postgrest.exceptions.APIError as e:
            print("postgrest.exceptions.APIError")
            print(e.args)
        return []

# シティリーグ使用されたIDを表示
class cityLeagueLister:
    def getList4w(self):
        array_id_4w = []
        with open("event/cl.json", "r", encoding="utf_8_sig") as f:
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

    def getList(self):
        array_id = []
        with open("event/cl.json", "r", encoding="utf_8_sig") as f:
            data = json.load(f)
        if 'items' in data:
            for item in data['items']:
                if 'index' in item:
                    if item['index'] >= 0 and item['index'] < 4:
                        if 'card_list' in item:
                            for card in item['card_list']:
                                if 'card_id' in card:
                                    array_id.append(card['card_id'])
        array_id = list(set(array_id))
        return array_id

# 採用情報付きの価格情報を生成する
class cityLeagueDeckCardProvider:
    def _get(self):
        df = pd.DataFrame(index=[], columns=['card_id','count','card_id_rows'])
        with open("event/cl.json", "r", encoding="utf_8_sig") as f:
            data = json.load(f)
        if 'items' in data:
            for item in data['items']:
                if 'index' in item:
                    if item['index'] == 0:
                        if 'card_list' in item:
                            df = pd.DataFrame(item['card_list'])
        return df

    def _getDeckCount(self):
        with open("event/cl.json", "r", encoding="utf_8_sig") as f:
            data = json.load(f)
        if 'items' in data:
            for item in data['items']:
                if 'index' in item:
                    if item['index'] == 0:
                        if 'deck_count' in item:
                            return item['deck_count']
        return 0

    def get(self, df: pd.DataFrame):
        count = self._getDeckCount()
        partDf = self._get()
        if len(partDf) > 0:
            partDf = partDf.rename(columns={
                'card_id': 'official_id',
                'count': 'cl_count',
                'card_id_rows': 'cl_deck',
                })
            newDf = pd.merge(df,partDf,how='left',on='official_id')
        else:
            newDf = df
            newDf['cl_count'] = 0
            newDf['cl_deck'] = 0
        newDf = newDf.fillna({'cl_count': 0, 'cl_deck': 0})
        if count > 0:
            newDf['cl_rate'] = newDf['cl_deck'] / count * 100
            newDf['cl_rate'] = newDf['cl_rate'].round(2)
        else:
            newDf['cl_rate'] = 0
        return newDf

# イベントのインデックスを生成する
class eventIdIndexGen:
    def _getIndex(self):
        temp = []
        with open("event/cl.json", "r", encoding="utf_8_sig") as f:
            data = json.load(f)
        if 'items' in data:
            for item in data['items']:
                if 'index' in item:
                    if item['index'] == 0:
                        if 'deck_list' in item:
                            for deck in item['deck_list']:
                                if deck['event_id'] not in temp:
                                    temp.append(deck['event_id'])
        return temp

    def output(self, file_name: str):
        dict_tmp = {'items':self._getIndex()}
        with open(file_name, 'w', encoding='utf_8_sig') as f:
            json.dump(dict_tmp, f, ensure_ascii=False)

# デッキタイプのインデックスを生成する
class deckTypeIndexGen:
    def _getIndex(self):
        temp = []
        with open("event/cl.json", "r", encoding="utf_8_sig") as f:
            data = json.load(f)
        if 'items' in data:
            for item in data['items']:
                if 'index' in item:
                    if item['index'] == 0:
                        if 'deck_list' in item:
                            for deck in item['deck_list']:
                                if deck['deck_type'] not in temp:
                                    temp.append(deck['deck_type'])
        return temp

    def output(self, file_name: str):
        dict_tmp = {'items':self._getIndex()}
        with open(file_name, 'w', encoding='utf_8_sig') as f:
            json.dump(dict_tmp, f, ensure_ascii=False)


# シティリーグで使用されたデッキの分類をカウントする
class cityLeagueDeckCounter:
    def get(self, rank1: bool):
        df = self._getDf()
        if rank1:
            df = df[df['rank'] == 1]
        grouped = df.groupby('deck_type')
        array = []
        for deck_type, group in grouped:
            array.append({
                'name': deck_type,
                'value': len(group)
            })
        array.sort(key=lambda x: x['value'], reverse=True)
        df = pd.DataFrame(array)
        return df

    def _getDf(self):
        with open("event/cl.json", "r", encoding="utf_8_sig") as f:
            data = json.load(f)
        if 'items' in data:
            for item in data['items']:
                if 'index' in item:
                    if item['index'] == 0:
                        if 'deck_list' in item:
                            return pd.DataFrame(item['deck_list'])
        return None

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



# デッキ画像を生成するための構成情報を出力する
class cityLeagueDeckRecipeProvider:
    all_recipe = False
    only_rank1 = False
    event_id = False
    priceDf = None

    def __init__(self,priceDf):
        self.all_recipe = False
        self.only_rank1 = False
        self.event_id = False
        self.priceDf = priceDf

    def get(self):
        temp_dict = {'items':[]}
        with open("event/cl.json", "r", encoding="utf_8_sig") as f:
            data = json.load(f)
        if 'items' in data:
            for item in data['items']:
                if 'index' in item:
                    if item['index'] == 0:
                        if 'deck_list' in item:
                            df = pd.DataFrame(item['deck_list'])
                            if self.all_recipe == True:
                                return self._getAll(item, df)
                            if self.event_id == True:
                                return self._getEvent(item, df)
                            if self.only_rank1 == True:
                                return self._getRank1(item, df)
                            else:
                                return self._getGroupType(item, df)
        return temp_dict

    def _getAll(self, item, df: pd.DataFrame):
        temp_dict = {'items':[]}
        temp_dict['items'] = self._getDeckList(item, df.to_dict(orient='records'))
        return temp_dict

    def _getEvent(self, item, df: pd.DataFrame):
        temp_dict = {'items':[]}
        grouped = df.groupby('event_id')
        for event_id, group in grouped:
            temp_dict['items'].append({
                'event_id': event_id,
                'count': len(group),
                'items': self._getDeckList(item, group.to_dict(orient='records'))
            })
        return temp_dict

    def _getRank1(self, item, df: pd.DataFrame):
        temp_dict = {'items':[]}
        df = df[df['rank'] == 1]
        temp_dict['items'].append({
            'count': len(df),
            'items': self._getDeckList(item, df.to_dict(orient='records'))
        })
        return temp_dict

    def _getGroupType(self, item, df: pd.DataFrame):
        temp_dict = {'items':[]}
        grouped = df.groupby('deck_type')
        for deck_type, group in grouped:
            temp_dict['items'].append({
                'deck_type': deck_type,
                'count': len(group),
                'items': self._getDeckList(item, group.to_dict(orient='records'))
            })
        return temp_dict

    def save(self, data: dict, file_name: str):
        with open(file_name, 'w', encoding='utf_8_sig') as f:
            json.dump(data, f, ensure_ascii=False)

    def _getDeckList(self, fileItem, decks):
        l = []
        for deck in decks:
            write_deck_info = {
                'deck_id': deck['deck_id'],
                'event_id': deck['event_id'],
                'deck_type': deck['deck_type'],
                'datetime': deck['datetime'],
                'rank': deck['rank'],
                'event_name': deck['event_name'],
                'sponsorship': deck['sponsorship'],
                'player_name': deck['player_name'],
                'items': self._getRecipe(fileItem,deck['deck_id']),
                'count': self._getRecipeCount(fileItem,deck['deck_id']),
            }
            write_deck_info['card_type'] = self._getDeckCardType(write_deck_info['items'])
            write_deck_info['regulation'] = self._getDeckRegulation(write_deck_info['items'])
            write_deck_info['price'] = self._getDeckPrice(write_deck_info['items'])
            l.append(write_deck_info)
        if len(l) > 0:
            data = pd.DataFrame()
            df = data.from_dict(l)
            df = df.sort_values(by=['rank','datetime'], ascending=[True,False])
            l = df.to_dict(orient="record")
        return l

    def _getDeckRegulation(self, deck_items):
        counts = {}
        total = 0
        for d in deck_items:
            if d['regulation'] in counts:
                counts[d['regulation']] += d['count']
            else:
                counts[d['regulation']] = d['count']
            total += d['count']
        result = []
        for reg, count in counts.items():
            result.append({'mark': reg, 'count':count, 'ratio': round(count / total * 100)})
        result = sorted(result, key=lambda x: x['ratio'], reverse=True)
        total_ratio = 0
        for item in result:
            total_ratio += item['ratio']
        if total_ratio > 100:
            result[0]['ratio'] -= (total_ratio - 100)
            result[0]['ratio'] = round(result[0]['ratio'])
        elif total_ratio < 100:
            result[0]['ratio'] += (100 - total_ratio)
            result[0]['ratio'] = round(result[0]['ratio'])
        data = pd.DataFrame()
        df = data.from_dict(result)
        dfD = df[df['mark'] == 'D']
        dfE = df[df['mark'] == 'E']
        dfF = df[df['mark'] == 'F']
        dfG = df[df['mark'] == 'G']
        dfH = df[df['mark'] == 'H']
        dfI = df[df['mark'] == 'I']
        dfNan = df[df['mark'] == '']
        df = pd.concat([dfD,dfE,dfF,dfG,dfH,dfI,dfNan])
        result = df.to_dict(orient="record")
        return result

    def _getDeckCardType(self, deck_items):
        counts = {}
        total = 0
        for d in deck_items:
            temp_type = d['card_type']
            if temp_type != 'P' and temp_type != 'E':
                temp_type = 'T'
            if temp_type in counts:
                counts[temp_type] += d['count']
            else:
                counts[temp_type] = d['count']
            total += d['count']
        result = []
        for reg, count in counts.items():
            result.append({'mark': reg, 'count':count, 'ratio': round(count / total * 100)})
        result = sorted(result, key=lambda x: x['ratio'], reverse=True)
        total_ratio = 0
        for item in result:
            total_ratio += item['ratio']
        if total_ratio > 100:
            result[0]['ratio'] -= (total_ratio - 100)
            result[0]['ratio'] = round(result[0]['ratio'])
        elif total_ratio < 100:
            result[0]['ratio'] += (100 - total_ratio)
            result[0]['ratio'] = round(result[0]['ratio'])
        data = pd.DataFrame()
        df = data.from_dict(result)
        dfP = df[df['mark'] == 'P']
        dfT = df[df['mark'] == 'T']
        dfE = df[df['mark'] == 'E']
        df = pd.concat([dfP,dfT,dfE])
        result = df.to_dict(orient="record")
        return result

    def _getDeckPrice(self, deck_items):
        total = 0
        diff = 0
        zero = 0
        zero_count = 0
        for d in deck_items:
            if d['price']['latest'] == 0:
                zero += 1
                zero_count += d['count']
            else:
                total += d['price']['latest'] * d['count']
                diff += d['price']['diff_7d'] * d['count']
        result = {
            'total': total, 
            'diff_7d': diff,
            'percent_7d': round(((total / (total - diff)) - 1) * 100, 1),
            'zero': zero,
            'zero_count': zero_count
        }
        return result

    def _getPrice(self, master_id: str):
        df = self.priceDf[self.priceDf['master_id'] == master_id]
        if len(df) == 0:
            return {'latest': 0, 'diff_7d': 0, 'percent_7d': 0}
        dict = df.iloc[0].to_dict()
        return {'latest': dict['latest_price'], 'diff_7d': dict['diff_7d'], 'percent_7d': dict['percent_7d']}

    def _getRecipe(self, item, deck_id: str):
        if item == None:
            return []
        if 'deck_recipe' in item:
            if deck_id in item['deck_recipe']:
                if 'items' in item['deck_recipe'][deck_id]:
                    return self._mergePrice(self._sortedPoke(item['deck_recipe'][deck_id]['items']))
        return []

    def _sortedPoke(self, l):
        if len(l) > 0:
            data = pd.DataFrame()
            df = data.from_dict(l)
            dfP = df[df['card_type'] == 'P'].sort_values(by=['count','card_id'], ascending=[False,False])
            dfG = df[df['card_type'] == 'G'].sort_values(by=['count','card_id'], ascending=[False,False])
            dfT = df[df['card_type'] == 'T'].sort_values(by=['count','card_id'], ascending=[False,False])
            dfS = df[df['card_type'] == 'S'].sort_values(by=['count','card_id'], ascending=[False,False])
            dfD = df[df['card_type'] == 'D'].sort_values(by=['count','card_id'], ascending=[False,False])
            dfE = df[df['card_type'] == 'E'].sort_values(by=['count','card_id'], ascending=[False,False])
            df = pd.concat([dfP,dfG,dfT,dfS,dfD,dfE])
            l = df.to_dict(orient="record")
        return l

    def _mergePrice(self, l):
        for item in l:
            item['price'] = self._getPrice(item['master_id'])
        return l

    def _getRecipeCount(self, item, deck_id):
        count = 0
        if item == None:
            return 0
        if 'deck_recipe' in item:
            if deck_id in item['deck_recipe']:
                if 'items' in item['deck_recipe'][deck_id]:
                    for item in item['deck_recipe'][deck_id]['items']:
                        count += item['count']
        return count

class cityLeagueEventDeckRecipeProvider(cityLeagueDeckRecipeProvider):
    def __init__(self,priceDf):
        super().__init__(priceDf)
        self.all_recipe = False
        self.only_rank1 = False
        self.event_id = True

class cityLeagueAllDeckRecipeProvider(cityLeagueDeckRecipeProvider):
    def __init__(self,priceDf):
        super().__init__(priceDf)
        self.all_recipe = True
        self.only_rank1 = False
        self.event_id = False

class cityLeagueRank1DeckRecipeProvider(cityLeagueDeckRecipeProvider):
    def __init__(self,priceDf):
        super().__init__(priceDf)
        self.all_recipe = False
        self.only_rank1 = True
        self.event_id = False


# ログを生成
class logGen:
    expansion_files = "./log/*.json"

    def _readFileDf(self, file_name: str):
        dfMarket = pd.DataFrame(index=[], columns=['market','link','price','name','date','stock'])
        with open(file_name, encoding='utf_8_sig') as f:
            dict_tmp = {}
            data = json.load(f)
            counter = 0
            for item in data['items']:
                dict_tmp[counter] = {
                    'market':item['market'],
                    'link':item['link'],
                    'price':item['price'],
                    'name':item['name'],
                    'date':item['date'],
                    'stock':item['stock']
                }
                counter += 1
            dfMarket = dfMarket.from_dict(dict_tmp, orient="index")
        return dfMarket

    def _getHeadRecord(self, df: pd.DataFrame):
        df = df.sort_values(by=['date','price','stock'], ascending=[False,True,False])
        return df

    def _getSummaryRecord(self, df: pd.DataFrame):
        lowDf = df.sort_values(by=['date','price'], ascending=[False,True]).head(1)
        stockDf = df.sort_values(by=['date','stock'], ascending=[False,False]).head(2)
        writeDf = pd.concat([lowDf, stockDf])
        writeDf = writeDf[~writeDf.duplicated(keep='last', subset=['date','link'])]
        return writeDf

    def output(self, file_name: str):
        dict_tmp = {'items':[]}
        files = glob.glob(self.expansion_files)
        print('output log')
        for file in files:
            masterid = os.path.splitext(os.path.basename(file))[0]
            df = self._readFileDf(file)
            #print(masterid)
            if len(df) == 0: 
                dict_tmp['items'].append({
                    'master_id': masterid,
                    'log': []
                })
                continue
            df = df.sort_values(by=['date'], ascending=[False])
            dfMagi = self._getSummaryRecord(df[df['market'] == 'magi'])
            dfHare = self._getHeadRecord(df[df['market'] == 'hareruya2']).head(1)
            dfCaRu = self._getHeadRecord(df[df['market'] == 'cardrush']).head(1)
            dfToCo = self._getHeadRecord(df[df['market'] == 'torecolo']).head(1)
            writeDf = pd.concat([dfMagi, dfHare, dfCaRu, dfToCo])
            writeDf = writeDf.sort_values(by=['date','price','market'], ascending=[False,True,True])
            dict_tmp['items'].append({
                'master_id': masterid,
                'log': writeDf.to_dict(orient="record")
            })
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

# 型安全なデータを生成する
class safeTypeDfFactory:
    def get(self, df: pd.DataFrame):
        # 現在の型
        #print(df.dtypes)
        print(df.info())
        return df

# チャンクデータを生成する
class chunkDfFactory:
    def get(self, df: pd.DataFrame):
        df = df.drop(columns={'price_list_hy'})
        chunks = [df[i:i+2000] for i in range(0, len(df), 2000)]
        return chunks

    def save(self, chunks: pd.DataFrame, chunk_dir: str):
        files = glob.glob(chunk_dir+'*/.json')
        for file in files:
            os.remove(file)
        for i, chunk in enumerate(chunks):
            self._save(chunk,chunk_dir+f'/all_chunk_{i}.json')

    def _save(self, df: pd.DataFrame, file_name: str):
        dict_tmp = {'items': []}
        dict_tmp['items'] = df.to_dict(orient="record")
        with open(file_name, 'w', encoding='utf_8_sig') as f:
            json.dump(dict_tmp, f, ensure_ascii=False)

# マスターIDのインデックスを生成する
class masterIdIndexGen:
    def output(self, df: pd.DataFrame, file_name: str):
        col_array = df['master_id'].to_numpy().tolist()
        dict_tmp = {'items': col_array}
        with open(file_name, 'w', encoding='utf_8_sig') as f:
            json.dump(dict_tmp, f, ensure_ascii=False)
       
# 価格情報の基本情報を生成
class dailyPriceFactory:
    market_files = "./market/*.json"

    def get(self):
        listWeekly = {}
        listDaily = {}
        listStock = {}
        listWeeklyPriceList = {}
        listHalfYearPriceList = {}
        files = glob.glob(self.market_files)
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
                halfYearPriceList = self.getHalfYearPriceList(data)
                if halfYearPriceList is not None:
                    listHalfYearPriceList[len(listHalfYearPriceList)] = {
                        'master_id': masterid,
                        'price_list_hy': halfYearPriceList,
                    }

        dfDaily = pd.DataFrame.from_dict(listDaily, orient='index')
        dfWeekly = pd.DataFrame.from_dict(listWeekly, orient='index')
        dfStock = pd.DataFrame.from_dict(listStock, orient='index')
        dfWeeklyPriceList = pd.DataFrame.from_dict(listWeeklyPriceList, orient='index')
        dfHalfYearPriceList = pd.DataFrame.from_dict(listHalfYearPriceList, orient='index')

        dfDaily = pd.merge(dfDaily,dfWeekly,how='inner',on='master_id')
        dfDaily = pd.merge(dfDaily,dfStock,how='inner',on='master_id')
        dfDaily = pd.merge(dfDaily,dfWeeklyPriceList,how='inner',on='master_id')
        dfDaily = pd.merge(dfDaily,dfHalfYearPriceList,how='inner',on='master_id')
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

        card_list = []
        files = glob.glob("./card/*.csv")
        for file in files:
            readDf = pd.read_csv(
                file,
                encoding="utf_8_sig", sep=",",
                header=0)
            card_list.append(readDf)
        readDfAll = pd.concat(card_list, axis=0, ignore_index=True, sort=True)
        dfDaily = pd.merge(dfDaily,readDfAll,how='inner',on='master_id')

        return dfDaily

    def readSupabase(self, supabase:Client):
        indexReader = cardSummaryIndexReader()
        summaryReader = cardSummaryReader()
        id_list = indexReader.read(supabase)
        for i in range(0, len(id_list), 100):
            batch = id_list[i: i+100]
            print('Write log no.:'+str(i))
            data = summaryReader.read(supabase,batch)
            for j in data:
                buffer = {"price":{}}
                buffer['price'] = j['card_price']
                with open('./market/'+j['master_id']+'.json', 'w') as f:
                    json.dump(buffer, f, indent=4)

    def getNewRegulation(self,df:pd.DataFrame):
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
        return (self._getDurationPriceList(data,'weekly'))

    def getHalfYearPriceList(self,data):
        return (self._getDurationPriceList(data,'halfYear'))

    # duration : halfYear / weekly
    def _getDurationPriceList(self,data,duration):
        if 'price' in data:
            if duration in data['price'] and data['price'][duration] is not None:
                durationData = data['price'][duration]
                if 'archive' in durationData and durationData['archive'] is not None:
                    archive = durationData['archive']
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
        dfPrice.loc[dfPrice.copyright == 'n/a','copyright']=False
        dfPrice = dfPrice.fillna({'percent_24h': 0, 'percent_7d': 0})
        dfPrice = dfPrice.fillna('n/a')
        dfPrice = self._filterDuplicatedRank(dfPrice)
        dfPrice = dfPrice.sort_values(by=['rank','latest_price'], ascending=[True,False])
        return dfPrice

    def rePriceRank(self, dfList: pd.DataFrame):
        dfPrice = self._filterIDList(dfList)
        dfPrice = self._filterExpansion(dfPrice)
        dfPrice = self._filterRegulation(dfPrice)
        dfPrice = self._addRank(dfPrice)

        dfPrice['rarity'] = dfPrice['rarity'].str.replace('-','n/a')
        dfPrice = dfPrice.replace([np.inf, -np.inf], 0.0)
        dfPrice.loc[dfPrice.percent_24h == 'n/a','percent_24h']=0
        dfPrice.loc[dfPrice.percent_7d == 'n/a','percent_7d']=0
        dfPrice.loc[dfPrice.is_mirror == 'n/a','is_mirror']=False
        dfPrice.loc[dfPrice.copyright == 'n/a','copyright']=False
        dfPrice = dfPrice.fillna({'percent_24h': 0, 'percent_7d': 0})
        dfPrice = dfPrice.fillna('n/a')
        dfPrice = self._filterDuplicatedRank(dfPrice)
        dfPrice = dfPrice.sort_values(by=['rank','latest_price'], ascending=[True,False])
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

# 値動きを取得
class priceChartDataProvider:
    def getData(self, df: pd.DataFrame):
        l = []
        for id, row in df.iterrows():
            l.append(self._get(row))
        return {'items': l}
    
    def _getRecords(self, row: pd.Series, duration: str):
        item = row[duration]
        records = list()
        for i in range(len(item['items'])):
            records.append({
                'date' : None,
                'p50': None,
                'min': None,
                'stock': None,
            })

        for i, dateData in enumerate(item['items']):
            dt = datetime.datetime.strptime(dateData['datetime'], "%Y-%m-%d %H:%M:%S")
            records[i]['date'] = dt.strftime("%Y-%m-%d")
            records[i]['p50'] = dateData['p50']
            records[i]['min'] = dateData['min']
            records[i]['stock'] = dateData['stock']
        return records
    
    def _get(self, row: pd.Series):
        legend = {
            'master_id' : row['master_id'],
            'rank': row['rank'],
            'name': row['name'],
            'expansion': row['expansion'],
            'expansion_name': row['expansion_name'],
            'rarity': row['rarity'],
            'cn': row['cn'],
            'card_type': row['card_type'],
            'sub_type': row['sub_type'],
        }
        
        dict_tmp = {
            'master_id': row['master_id'],
            'chart_line' : {
                'legend': legend,
                'items_7d': self._getRecords(row,'price_list_7d'),
                'items_hy': self._getRecords(row,'price_list_hy')
            }
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


url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_ANON_KEY")
service_key: str = os.environ.get("SUPABASE_SERVICE_KEY")

supabase: Client = create_client(url, key)
supabase.postgrest.auth(service_key)

Path('./market').mkdir(parents=True, exist_ok=True)
output_dir = './dist'
Path(output_dir).mkdir(parents=True, exist_ok=True)
rank_dir = output_dir+'/rank'
Path(rank_dir).mkdir(parents=True, exist_ok=True)
#chunk_dir = rank_dir+'/chunk'
#Path(chunk_dir).mkdir(parents=True, exist_ok=True)
recipe_dir = output_dir+'/recipe'
Path(recipe_dir).mkdir(parents=True, exist_ok=True)
chart_dir = output_dir+'/chart'
Path(chart_dir).mkdir(parents=True, exist_ok=True)
#index_dir = output_dir+'/index'
#Path(index_dir).mkdir(parents=True, exist_ok=True)
log_dir = output_dir+'/log'
Path(log_dir).mkdir(parents=True, exist_ok=True)

exp_fact = expansionFactory()
daily_fact = dailyPriceFactory()
expDf = exp_fact.get()
dailyDf = daily_fact.readSupabase(supabase)
dailyDf = daily_fact.get()
dailyDf = daily_fact.getNewRegulation(dailyDf)

# 全カード対象
topCalc = calcTopPrice()
ranks = rankCalculator()
priceDf = ranks.getPriceRank(dailyDf,expDf)
clCard = cityLeagueDeckCardProvider()
priceDf = clCard.get(priceDf)

safeFact= safeTypeDfFactory()
priceDf = safeFact.get(priceDf)

# 検索用チャンクデータ生成 -> 廃止
#chunkFact = chunkDfFactory()
#chunksDf = chunkFact.get(priceDf)
#chunkFact.save(chunksDf,chunk_dir)

# インデックス生成 -> 廃止
#indexer = masterIdIndexGen()
#indexer.output(priceDf,index_dir+'/id.json')

ranks.save(priceDf,rank_dir+'/all.json')
#ranks.save(priceDf.head(50),rank_dir+'/all_head.json')

pchartProvider = priceChartDataProvider()
pchartProvider.save(pchartProvider.getData(priceDf), chart_dir+'/all_line_charts.json')

# 監査情報
audit = auditStockLogger()
audit.saveRegulation(priceDf, rank_dir+'/all_stock_regu.json')
audit.saveRarity(priceDf, rank_dir+'/all_stock_rarity.json')

# CLデッキレシピ生成
#recipeProvider = cityLeagueDeckRecipeProvider(priceDf)
#recipeProvider.save(recipeProvider.get(), recipe_dir+'/deck_recipe.json')
#recipeProvider = cityLeagueRank1DeckRecipeProvider(priceDf)
#recipeProvider.save(recipeProvider.get(), recipe_dir+'/deck_recipe_rank1.json')
#recipeProvider = cityLeagueEventDeckRecipeProvider(priceDf)
#recipeProvider.save(recipeProvider.get(), recipe_dir+'/deck_recipe_event.json')
recipeProvider = cityLeagueAllDeckRecipeProvider(priceDf)
recipeProvider.save(recipeProvider.get(), recipe_dir+'/deck_recipe_all.json')
del priceDf
gc.collect()  

# ---------------------

ranks.is_filtered_dupcard = True
priceDf = ranks.getPriceRank(dailyDf,expDf)
topDf = topCalc.get7daysTopPrice(priceDf)
topCalc.save(topDf, rank_dir+'/all_price_top.json')

ranks.rank_price_type = 'percent_24h'
ranks.is_filtered_dupcard = True
priceDf = ranks.getPriceRank(dailyDf,expDf)
topDf = topCalc.get7daysTopPrice(priceDf)
topCalc.save(topDf, rank_dir+'/all_price_rise_24h_top.json')

del topDf
gc.collect() 
# ---------------------

# シティリーグ対象
topCalc = calcTopPrice()
ranksCL = rankCalculator()
ranksCL.rank_price_type = 'percent_7d'
ranksCL.is_filtered_dupcard = True
priceDf = ranksCL.getPriceRank(dailyDf,expDf)
priceDf = clCard.get(priceDf)
priceDf = ranksCL.rePriceRank(priceDf[priceDf['cl_count'] > 0])
topDf = topCalc.get7daysTopPrice(priceDf)
topCalc.save(topDf, rank_dir+'/cl_price_rise_top.json')

counterCL = cityLeagueDeckCounter()
counterDf = counterCL.get(False)
counterCL.save(counterDf,rank_dir+'/cl_deck_top.json')
counterDf = counterCL.get(True)
counterCL.save(counterDf,rank_dir+'/cl_deck_rank1_top.json')

del topDf
del priceDf
del counterDf
gc.collect() 

# ---------------------

# 新弾対象
topCalc = calcTopPrice()
ranksNew = rankCalculator()
ranksNew.filtered_expansion = 'SV1S'
priceDf = ranksNew.getPriceRank(dailyDf,expDf)
topDf = topCalc.get7daysTopPrice(priceDf)
topCalc.save(topDf, rank_dir+'/new_product_price_top_sv1s.json')

ranksNew.filtered_expansion = 'SV1V'
priceDf = ranksNew.getPriceRank(dailyDf,expDf)
topDf = topCalc.get7daysTopPrice(priceDf)
topCalc.save(topDf, rank_dir+'/new_product_price_top_sv1v.json')

del topDf
del priceDf
gc.collect() 

# ---------------------

# デッキインデックス -> 廃止
#dtypeGen = deckTypeIndexGen()
#dtypeGen.output(index_dir+'/deck_type.json')

# イベントIDインデックス -> 廃止
#eventIdGen = eventIdIndexGen()
#eventIdGen.output(index_dir+'/event_id.json')

# ---------------------

# マーケットログ生成 -> 廃止
#log = logGen()
#log.output(log_dir+'/market_price_log.json')
