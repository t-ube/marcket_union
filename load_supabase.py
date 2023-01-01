import os
import json
import httpx
from supabase import create_client, Client

class supabaseReader:
    def read(self, supabase:Client):
        #data = supabase.table("card_summary").select("summary").eq("summary->>cn", '028/414').execute()
        #data = supabase.table("card_summary").select("summary").in_("summary->>cn", ['028/414']).execute()
        #data = supabase.table("card_summary").select("summary").ilike("summary->>name", 'かがやくゲ*').execute()
        data = supabase.table("card_summary").select("summary").ilike("summary->>name", 'ミュウVM*').limit(1).execute()
        print(data)

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_ANON_KEY")
supabase: Client = create_client(url, key)

reader = supabaseReader()
reader.read(supabase)
