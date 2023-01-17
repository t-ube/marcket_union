import os
import json
import httpx
import postgrest
import datetime
from supabase import create_client, Client

class supabaseWriter:
    def writeSummary(self, supabase:Client):
        timestamp = datetime.datetime.utcnow()
        with open("./dist/rank/all.json", "r", encoding="utf_8_sig") as f:
            data = json.load(f)
        if 'items' in data:
            # 100 個ずつに分解する
            for i in range(0, len(data['items']), 100):
                batch = data['items'][i: i+100]
                print('Write summary no.:'+str(i))
                batch_item = []
                batch_master_id = []
                for item in batch:
                    if item['master_id'] in batch_master_id:
                        print('Already exists: '+item['master_id'])
                    else:
                        batch_item.append({
                            "master_id": item['master_id'],
                            "summary": item,
                            "updated_at": timestamp.strftime('%Y-%m-%d %H:%M:%S+00'),
                        })
                        batch_master_id.append(item['master_id'])
                try:
                    supabase.table("card_summary").upsert(batch_item).execute()
                except httpx.ReadTimeout:
                    print("httpx.ReadTimeout")
                except httpx.WriteTimeout as e:
                    print("httpx.WriteTimeout")
                    print(e.args)
                except postgrest.exceptions.APIError as e:
                    print("postgrest.exceptions.APIError")
                    print(e.args)
                    print('Begin error data')
                    print(batch_item)
                    print('End error data')

    def writeRecipe(self, supabase:Client):
        timestamp = datetime.datetime.utcnow()
        with open("./dist/recipe/deck_recipe_all.json", "r", encoding="utf_8_sig") as f:
            data = json.load(f)
        if 'items' in data:
            # 20 個ずつに分解する
            for i in range(0, len(data['items']), 20):
                batch = data['items'][i: i+20]
                print('Write recipe no.:'+str(i))
                batch_item = []
                batch_deck_id = []
                for item in batch:
                    if item['deck_id'] in batch_deck_id:
                        print('Already exists: '+item['deck_id'])
                    else:
                        batch_item.append({
                            "deck_id": item['deck_id'],
                            "event_id": item['event_id'],
                            "deck_type": item['deck_type'],
                            "datetime": item['datetime']+"+09",
                            "rank": item['rank'],
                            "event_name": item['event_name'],
                            "sponsorship": item['sponsorship'],
                            "player_name": item['player_name'],
                            "items": item['items'],
                            "updated_at": timestamp.strftime('%Y-%m-%d %H:%M:%S+00'),
                            "count": item['count'],
                            "card_type": item['card_type'],
                            "regulation": item['regulation'],
                            "price": item['price']
                        })
                        batch_deck_id.append(item['deck_id'])
                try:
                    supabase.table("deck_recipe").upsert(batch_item).execute()
                except httpx.ReadTimeout:
                    print("httpx.ReadTimeout")
                except httpx.WriteTimeout as e:
                    print("httpx.WriteTimeout")
                    print(e.args)
                except postgrest.exceptions.APIError as e:
                    print("postgrest.exceptions.APIError")
                    print(e.args)
                    print('Begin error data')
                    print(batch_item)
                    print('End error data')
    
    def writeChart(self, supabase:Client):
        timestamp = datetime.datetime.utcnow()
        with open("./dist/chart/all_line_charts.json", "r", encoding="utf_8_sig") as f:
            data = json.load(f)
        if 'items' in data:
            # 100 個ずつに分解する
            for i in range(0, len(data['items']), 100):
                batch = data['items'][i: i+100]
                print('Write chart no.:'+str(i))
                batch_item = []
                batch_master_id = []
                for item in batch:
                    if item['master_id'] in batch_master_id:
                        print('Already exists: '+item['master_id'])
                    else:
                        batch_item.append({
                            "master_id": item['master_id'],
                            "chart_line": item['chart_line'],
                            "updated_at": timestamp.strftime('%Y-%m-%d %H:%M:%S+00'),
                        })
                        batch_master_id.append(item['master_id'])
                try:
                    supabase.table("card_chart").upsert(batch_item).execute()
                except httpx.ReadTimeout:
                    print("httpx.ReadTimeout")
                except httpx.WriteTimeout as e:
                    print("httpx.WriteTimeout")
                    print(e.args)
                except postgrest.exceptions.APIError as e:
                    print("postgrest.exceptions.APIError")
                    print(e.args)
                    print('Begin error data')
                    print(batch_item)
                    print('End error data')

    def writeLog(self, supabase:Client):
        timestamp = datetime.datetime.utcnow()
        with open("./dist/log/market_price_log.json", "r", encoding="utf_8_sig") as f:
            data = json.load(f)
        if 'items' in data:
            # 100 個ずつに分解する
            for i in range(0, len(data['items']), 100):
                batch = data['items'][i: i+100]
                print('Write log no.:'+str(i))
                batch_item = []
                batch_master_id = []
                for item in batch:
                    if item['master_id'] in batch_master_id:
                        print('Already exists: '+item['master_id'])
                    else:
                        batch_item.append({
                            "master_id": item['master_id'],
                            "log": item['log'],
                            "updated_at": timestamp.strftime('%Y-%m-%d %H:%M:%S+00'),
                        })
                        batch_master_id.append(item['master_id'])
                try:
                    supabase.table("card_price_log").upsert(batch_item).execute()
                except httpx.ReadTimeout:
                    print("httpx.ReadTimeout")
                except httpx.WriteTimeout as e:
                    print("httpx.WriteTimeout")
                    print(e.args)
                except postgrest.exceptions.APIError as e:
                    print("postgrest.exceptions.APIError")
                    print(e.args)
                    print('Begin error data')
                    print(batch_item)
                    print('End error data')

    def clearFiles(self):
        os.remove('./dist/rank/all.json')
        os.remove('./dist/chart/all_line_charts.json')
        os.remove('./dist/recipe/deck_recipe_all.json')

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_ANON_KEY")
service_key: str = os.environ.get("SUPABASE_SERVICE_KEY")

supabase: Client = create_client(url, key)
supabase.postgrest.auth(service_key)

writer = supabaseWriter()
writer.writeSummary(supabase)
writer.writeRecipe(supabase)
writer.writeChart(supabase)
writer.clearFiles()
