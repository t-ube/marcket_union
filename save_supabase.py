import os
import json
import httpx
import datetime
from supabase import create_client, Client

class supabaseWriter:
    def writeSummary(self, supabase:Client):
        with open("./dist/rank/all.json", "r", encoding="utf_8_sig") as f:
            data = json.load(f)
        if 'items' in data:
            for item in data['items']:
                try:
                    supabase.table("card_summary").upsert(
                        {
                            "master_id": item['master_id'],
                            "summary": item
                        }
                    ).execute()
                except httpx.ReadTimeout:
                    print("httpx.ReadTimeout"+item['master_id'])

    def writeRecipe(self, supabase:Client):
        timestamp = datetime.datetime.utcnow()
        with open("./dist/recipe/deck_recipe_all.json", "r", encoding="utf_8_sig") as f:
            data = json.load(f)
        if 'items' in data:
            for item in data['items']:
                try:
                    supabase.table("deck_recipe").upsert(
                        {
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
                        }
                    ).execute()
                except httpx.ReadTimeout:
                    print("httpx.ReadTimeout"+item['deck_id'])
    
    def writeChart(self, supabase:Client):
        timestamp = datetime.datetime.utcnow()
        with open("./dist/chart/all_line_charts.json", "r", encoding="utf_8_sig") as f:
            data = json.load(f)
        if 'items' in data:
            for item in data['items']:
                try:
                    supabase.table("card_chart").upsert(
                        {
                            "master_id": item['master_id'],
                            "chart_line": item['chart_line'],
                            "updated_at": timestamp.strftime('%Y-%m-%d %H:%M:%S+00'),
                        }
                    ).execute()
                except httpx.ReadTimeout:
                    print("httpx.ReadTimeout"+item['master_id'])

    def writeLog(self, supabase:Client):
        timestamp = datetime.datetime.utcnow()
        with open("./dist/log/market_price_log.json", "r", encoding="utf_8_sig") as f:
            data = json.load(f)
        if 'items' in data:
            for item in data['items']:
                try:
                    supabase.table("card_price_log").upsert(
                        {
                            "master_id": item['master_id'],
                            "log": item['log'],
                            "updated_at": timestamp.strftime('%Y-%m-%d %H:%M:%S+00'),
                        }
                    ).execute()
                except httpx.ReadTimeout:
                    print("httpx.ReadTimeout"+item['master_id'])

    def clearFiles(self):
        os.remove('./dist/rank/all.json')
        os.remove('./dist/log/market_price_log.json')
        os.remove('./dist/chart/all_line_charts.json')
        os.remove('./dist/recipe/deck_recipe_all.json')


url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_ANON_KEY")
supabase: Client = create_client(url, key)

writer = supabaseWriter()
writer.writeSummary(supabase)
writer.writeRecipe(supabase)
writer.writeChart(supabase)
writer.writeLog(supabase)
writer.clearFiles()
