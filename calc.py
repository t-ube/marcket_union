import glob
import json
import os
import pandas as pd

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

listWeekly = {}
listDaily = {}
files = glob.glob("./marcket/*.json")
for file in files:
    with open(file, encoding='utf_8_sig') as f:
        masterid = os.path.splitext(os.path.basename(file))[0]
        data = json.load(f)
        priceW = priceWeekly(data,'50%')
        if priceW is not None:
            listWeekly[len(listWeekly)] = {
                'master_id': masterid,
                'base_price': priceW['basePrice'],
                'latest_price': priceW['latestPrice'],
                'percent': priceW['percent'],
            }
        priceD = priceDaily(data,'50%')
        if priceD is not None:
            listDaily[len(listDaily)] = {
                'master_id': masterid,
                'base_price': priceD['basePrice'],
                'latest_price': priceD['latestPrice'],
                'percent': priceD['percent'],
            }
dfWeekly = pd.DataFrame.from_dict(listWeekly, orient='index')
dfDaily = pd.DataFrame.from_dict(listDaily, orient='index')
print(dfWeekly.sort_values(by=['percent','latest_price'], ascending=[False,False]))
print(dfDaily.sort_values(by=['percent','latest_price'], ascending=[False,False]))
