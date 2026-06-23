import json
import os 
import sys
import pandas as pd 
from sqlalchemy import create_engine, text
from kafka import KafkaConsumer

DB_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://postgres:f1password@f1_postgres_server:5432/f1_analytics")
Kafka_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

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

#ingestion layer 
class F1DataConsumerPipeline: 
    def __init__(self, kafka_broker, task_mappings, db_connection_string):
        self.task_mappings = task_mappings 
        self.topics = list(task_mappings.keys())
        try: 
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=[kafka_broker],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            self.engine = create_engine(db_connection_string)
        
            with self.engine.begin() as conn: 
                conn.execute(text("""CREATE TABLE IF NOT EXISTS f1_raw_landing (
                    id SERIAL PRIMARY KEY,
                    topic VARCHAR(100),
                    raw_payload JSONB,
                    inserted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """))
        except Exception as e: 
            print(f"Kafka Consumer Initialization Error: {e}")
            send_notification("F1 Infrastructure Failure", "Kafka Consumer Initialization Error", str(e))
            raise ValueError(f"Kafka Consumer Initialization Error: {e}")
            
    def start_live_ingestion(self):
        print(f" Live Loader online. Processing topics: {self.topics}...")
        for message in self.consumer:
            try:
                raw_message = message.value
                current_topic = message.topic
                print(f" Received message from topic [{current_topic}]: {raw_message}")
                
                target_table = self.task_mappings.get(current_topic)
                if not target_table:
                    continue  # Skip if no mapping is found for the current topic
                
                #create and load a raw table for staging and error verification 
                with self.engine.begin() as conn:
                    conn.execute(
                        text("INSERT INTO f1_raw_landing (topic, raw_payload) VALUES (:topic, :payload)"),
                        {"topic": current_topic, "payload": json.dumps(raw_message)}
                    )
                
                df = pd.DataFrame([raw_message])
                if 'date_start' in df.columns:
                    df['date_start'] = pd.to_datetime(df['date_start'])
                
                date_column = ['date_start', 'date', 'date_end']
                for col in date_column:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col])
                        
                print(f" Ingesting record into clean table '{target_table}'")
                df.to_sql(
                    name=target_table, 
                    con=self.engine, 
                    if_exists='append', 
                    index=False
                )
                print(f" [OK] Successfully saved Raw and Clean entries for [{current_topic}]")            
                
            except Exception as loop_error:
                print(f" Error processing message: {loop_error}")
                send_notification("F1 Infrastructure Failure", "Live Ingestion Error", str(loop_error))
                continue  # Continue processing the next message even if there's an error

#actual worker           
if __name__ == "__main__":
    live_analytics_tasks = [
        {"topic": "live-car-telemetry", "table": "f1_clean_telemetry"},
        {"topic": "live-track-coordinates", "table": "f1_clean_coordinates"},
        {"topic": "live-team-audio", "table": "f1_clean_audio"},
        {"topic": "live-race-control", "table": "f1_clean_alerts"}
    ]
    print("=== STARTING LIVE CLEAN ANALYTICS CONSUMER CORE ===")
    try:
        mapping_dict = {task["topic"]: task["table"] for task in live_analytics_tasks}
        # 8. FIXED: Added missing commas between arguments
        worker = F1DataConsumerPipeline(
            kafka_broker=Kafka_BROKER,
            task_mappings=mapping_dict,
            db_connection_string=DB_URL
        )
        worker.start_live_ingestion()
    except Exception as system_error:
        send_notification("F1 Infrastructure Failure", "Live Consumer Pipeline Stopped", str(system_error))
        print(f"\n Live Consumer execution halted: {system_error}", file=sys.stderr)
