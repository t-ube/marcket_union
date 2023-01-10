import os
import httpx
import datetime
import postgrest
from supabase import create_client, Client 

class supabaseReader0110:
    def read(self, supabase:Client):
        try:
            data = supabase.table("test_0110").select("id").limit(10).execute()
        except httpx.ReadTimeout as e:
            print("httpx.ReadTimeout")
            print(e.args)
        except postgrest.exceptions.APIError as e:
            print("postgrest.exceptions.APIError")
            print(e.args)
        print(data)

class supabaseTableWriter0110:
    def write(self, supabase:Client):
        timestamp = datetime.datetime.utcnow()
        batch_item = []
        batch_item.append({"id": 3, "updated_at": timestamp.strftime('%Y-%m-%d %H:%M:%S+00')})
        try:
            supabase.table("test_0110").upsert(batch_item).execute()
        except httpx.WriteTimeout as e:
            print("httpx.WriteTimeout")
            print(e.args)
        except postgrest.exceptions.APIError as e:
            print("postgrest.exceptions.APIError")
            print(e.args)

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_ANON_KEY")
service_key: str = os.environ.get("SUPABASE_SERVICE_KEY")

supabase: Client = create_client(url, key)
print('------------Sign in-------------')
supabase.postgrest.auth(service_key)
print('------------Read----------------')
reader = supabaseReader0110()
reader.read(supabase)
print('------------Write---------------')
writer = supabaseTableWriter0110()
writer.write(supabase)
