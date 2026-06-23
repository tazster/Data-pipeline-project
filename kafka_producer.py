import json
import time
import fastf1 
import pandas as pd 
import os 
import sys
import requests
from sqlalchemy import create_engine
from kafka import KafkaProducer
from datetime import datetime, timezone

# database details locally - postgres and the API base URL for OpenF1
base_api_url = "https://api.openf1.org/v1"
DB_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://postgres:f1password@f1_postgres_server:5432/f1_analytics")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

# notify me of failure
def send_notification(title, subtitle, message):
    """Trigger a macOS banner alert if local, fallback to printing if in Docker"""
    print(f" ALERT: [{title} - {subtitle}] {message}")
    if sys.platform == "darwin":
        try:
            clean_message = str(message).replace('"', "'")
            applescript = f'display notification "{clean_message}" with title "{title}" subtitle "{subtitle}" sound name "Basso"'
            os.system(f'osascript -e "{applescript}"')
        except Exception as e:
            print(f"Could not trigger macOS notification layout: {e}") 

#producer class to send data to Kafka topics
class F1DataStreamer: 
    def __init__(self, api_url, kafka_broker, topic): 
        self.api_url = api_url 
        self.topic = topic
        try: 
            self.producer = KafkaProducer(
                bootstrap_servers=[kafka_broker],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all'  # Ensure all replicas acknowledge the message for durability
            )
        except Exception as e:
            print(f"Kafka Producer Initialization Error: {e}")
            send_notification("F1 Infrastructure Failure", "Kafka Producer Initialization Error", str(e))
            raise RuntimeError(f"Kafka Producer Initialization Error: {e}")
        
    def stream_data(self):
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        print(f" Fetching data from: {self.api_url}")
        response = requests.get(self.api_url, headers=headers, timeout=45)
        
        if response.status_code == 200: 
            data = response.json()
            if self.producer and data: 
                print(f" Pulled {len(data)} records from API. Streaming to topic {self.topic}...")
                for results in data:
                    self.producer.send(self.topic, value=results)
                
                self.producer.flush()
                print(f" Batch complete for topic: {self.topic}")
            elif not data:
                print(f" Warning: API returned empty list for topic: {self.topic}")
        else: 
            error_message = f"Status code {response.status_code} for URL: {self.api_url}"
            print(f"API error: {error_message}")
            send_notification("F1 Infrastructure Failure", "API Fetch Error", error_message)
            raise RuntimeError(f"API Fetch Error: {error_message}")

#main loop     
if __name__ == "__main__":
    print ("=== STARTING LIVE F1 PRODUCER PIPELINE ===")
    live_endpoint = [
    {"endpoint": "car_data", "topic": "live-car-telemetry"},
    {"endpoint": "location", "topic": "live-track-coordinates"},
    {"endpoint": "team_radio", "topic": "live-team-audio"},
    {"endpoint": "race_control", "topic": "live-race-control"}
]   
    while True:
        try:
            today_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            for task in live_endpoint:
                api_url = f"{base_api_url}/{task['endpoint']}?date_start>={today_date}"
            
                streamer = F1DataStreamer(api_url=api_url, kafka_broker=KAFKA_BROKER, topic=task["topic"])
                streamer.stream_data()
            
            print("\n Pausing for 5 seconds before next live telemetry sweep...\n")
            time.sleep(5)
            
        except Exception as system_error:
            send_notification("F1 Producer Failure", "Streaming Stopped", str(system_error))
            print(f"\n Producer execution halted: {system_error}", file=sys.stderr)
            time.sleep(10)  # Wait before retrying to avoid rapid failure loops