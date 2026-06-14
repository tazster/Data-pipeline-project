import json
import fastf1 
import pandas as pd 
import os 
import sys
import requests
from sqlalchemy import create_engine
from kafka import KafkaProducer, KafkaConsumer

# database details locally - postgres
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

class F1DataStreamer: 
    def __init__(self, api_url, kafka_broker, topic): 
        self.api_url = api_url 
        self.topic = topic
        try: 
            self.producer = KafkaProducer(
                bootstrap_servers=[kafka_broker],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        except Exception as e:
            print(f"Kafka Producer Initialization Error: {e}")
            self.producer = None
            raise
        
    def stream_data(self):  # <--- FIXED INDENTATION: Now properly nested inside the class
        # Adding a browser-like User-Agent to bypass OpenF1's bot block filters
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        print(f" Fetching data from: {self.api_url}")
        response = requests.get(self.api_url, headers=headers, timeout=45)
        
        if response.status_code == 200: 
            data = response.json()
            if self.producer and data: 
                # OpenF1 can return a lot of data; let's log the volume count
                print(f" Pulled {len(data)} records from API. Streaming to topic {self.topic}...")
                for r in data:
                    self.producer.send(self.topic, value=r)
                
                self.producer.flush()
                print(f" Batch complete for topic: {self.topic}")
            elif not data:
                print(f" Warning: API returned empty list for topic: {self.topic}")
        else: 
            print(f"API error: statuscode {response.status_code}")
            

class F1DataConsumerPipeline: 
    def __init__(self, kafka_broker, topic, db_connection_string, target_table):
        self.topic = topic 
        self.target_table = target_table
        try: 
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=[kafka_broker],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=5000
            )
            self.engine = create_engine(db_connection_string)
           
        except Exception as e: 
            print(f"Kafka Consumer Initialization Error: {e}")
            raise
            
    def process_topic(self):
        raw_messages = []
        if not self.consumer:
            print("no consumer connected")
            return
        for message in self.consumer:
            raw_messages.append(message.value)
            
        if not raw_messages:
            print(f"aborted: no data found in topic {self.topic}")
            return 
            
        staging_df = pd.DataFrame(raw_messages)
        cleaned_df = staging_df.copy()
        
        if 'date_start' in cleaned_df.columns:
            cleaned_df['date_start'] = pd.to_datetime(cleaned_df['date_start'])
            print(f"Data transformed in staging. Rows to load: {len(cleaned_df)}")
            
        try:
            print(f" Loading clean staging data directly into Postgres target table: {self.target_table}...")
            cleaned_df.to_sql(
                name=self.target_table, 
                con=self.engine, 
                if_exists='replace', 
                index=False
            )
            print(f"Success! Database analytics table '{self.target_table}' refreshed and live.")
        except Exception as e:
            print(f" Database Ingestion Failure on table {self.target_table}: {e}")
                

if __name__ == "__main__":
    pipeline_tasks = [
        {"endpoint": "meetings", "topic": "f1-meetings", "table": "f1_meetings", "params": "?year=2023"},
        {"endpoint": "sessions", "topic": "f1-sessions", "table": "f1_sessions", "params": "?year=2023"},
        {"endpoint": "meetings", "topic": "f1-meetings", "table": "f1_meetings", "params": "?year=2024"},
        {"endpoint": "sessions", "topic": "f1-sessions", "table": "f1_sessions", "params": "?year=2024"}
    ]
    
    print("=== STARTING ARCHITECTURAL F1 KAFKA PIPELINE ===")
    
    try:
        for task in pipeline_tasks:
            api_url = f"https://api.openf1.org/v1/{task['endpoint']}{task['params']}"
            streamer = F1DataStreamer(api_url=api_url, kafka_broker=KAFKA_BROKER, topic=task["topic"])
            streamer.stream_data()
            
        print("\n=== TRANSITIONING TO CONSUMPTION & STAGING LAYERS ===\n")
        
        for task in pipeline_tasks:
            pipeline_worker = F1DataConsumerPipeline(
                kafka_broker=KAFKA_BROKER, 
                topic=task["topic"], 
                db_connection_string=DB_URL, 
                target_table=task["table"]
            )
            pipeline_worker.process_topic()
            
    except Exception as system_error:
        send_notification("F1 Infrastructure Failure", "Pipeline Orchestration Stopped", str(system_error))
        print(f"\n Pipeline execution halted due to critical system failures: {system_error}", file=sys.stderr)