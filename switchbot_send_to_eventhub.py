import asyncio
import json
# *** CORRECTED IMPORT: Use the asynchronous client from .aio ***
from azure.eventhub.aio import EventHubProducerClient 
from azure.eventhub import EventData
import datetime
import time
import hashlib
import hmac
import base64
import uuid
import requests
import yaml

with open("config.yaml", "r") as stream:
    try:
        PARAM = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)


CONNECTION_STRING = PARAM["eventhub_connection_string"] 

apiHeader = {}
# open token
token = PARAM["switchbot_token"] # copy and paste from the SwitchBot app V6.14 or later
# secret key
secret = PARAM["switchbot_secret"]
nonce = uuid.uuid4()
t = int(round(time.time() * 1000))
string_to_sign = '{}{}{}'.format(token, t, nonce)

string_to_sign = bytes(string_to_sign, 'utf-8')
secret = bytes(secret, 'utf-8')

sign = base64.b64encode(hmac.new(secret, msg=string_to_sign, digestmod=hashlib.sha256).digest())
print ('Authorization: {}'.format(token))
print ('t: {}'.format(t))
print ('sign: {}'.format(str(sign, 'utf-8')))
print ('nonce: {}'.format(nonce))

#Build api header JSON
apiHeader['Authorization']=token
apiHeader['Content-Type']='application/json'
apiHeader['charset']='utf8'
apiHeader['t']=str(t)
apiHeader['sign']=str(sign, 'utf-8')
apiHeader['nonce']=str(nonce)

API_BASE_URL = "https://api.switch-bot.com"

def get_switchbot_hub_id():
    

    url = f"{API_BASE_URL}/v1.1/devices"

    r = requests.get(url, headers=apiHeader)

    for d in r.json()["body"]["deviceList"]:
        if d["deviceType"] == "Hub 2":
            return d["deviceId"]


async def send_data_point(device_id: str):
    # Create the producer client using the asynchronous class
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING, 
        # If the connection string is a full Event Hubs string, 
        # you might not need the eventhub_name argument.
        # eventhub_name=EVENTHUB_NAME 
    )
    
    # --- Data fetching and formatting logic ---
    
    # Example simulated data point (replace with your real data fetch)
    


    url = f"{API_BASE_URL}/v1.1/devices/{device_id}/status"
    

    try:
        r = requests.get(url, headers=apiHeader)
        event = r.json()["body"]

        current_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
        event["fetchedAt"] = current_time
            

            
        event_data_batch = await producer.create_batch()
        event_data_batch.add(EventData(json.dumps(event)))
        await producer.send_batch(event_data_batch)

        

    except Exception as e:
        print(f"Error fetching data: {e}")
        
    

async def run_data_stream(device_id: str):
    print("Starting real-time data stream...")
    while True:
        try:
            await send_data_point(device_id)
        except Exception as e:
            print(f"An error occurred during send: {e}")
            # Optional: Add error handling/retry logic here
        
        # Wait for 60 seconds before fetching and sending the next minute's price
        await asyncio.sleep(60) 

# Run the async function
if __name__ == "__main__":
    try:
        device_id = get_switchbot_hub_id()
        asyncio.run(run_data_stream(device_id))
    except KeyboardInterrupt:
        print("\nData stream stopped by user.")