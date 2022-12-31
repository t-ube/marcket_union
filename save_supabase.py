import os
from supabase import create_client, Client

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_ANON_KEY")
supabase: Client = create_client(url, key)
data = supabase.table("card_summary").insert({"master_id":"xxx","summary":{"data1":"abc","data2":123,"array":[1,2,3]}}).execute()
assert len(data.data) > 0
