import os
import json
import httpx
from supabase import create_client, Client

class supabaseWriter:
    def write(self, supabase:Client):
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

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_ANON_KEY")
supabase: Client = create_client(url, key)

writer = supabaseWriter()
writer.write(supabase)
