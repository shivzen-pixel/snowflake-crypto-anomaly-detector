import websocket
import json
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from snowflake.ingest import SimpleIngestManager
from snowflake.ingest import StagedFile
from snowflake.connector import connect

load_dotenv()

# Products to watch
PRODUCTS = ["BTC-USD", "ETH-USD", "SOL-USD"]

# Snowflake connection
def get_snowflake_connection():
    return connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )

# Write to Snowflake directly
def write_to_snowflake(conn, table, rows):
    if not rows:
        return
    cursor = conn.cursor()
    for row in rows:
        cursor.execute(f"""
            INSERT INTO RAW.{table} (received_at, channel, sequence_num, raw_payload)
            SELECT 
                '{row['received_at']}'::TIMESTAMP_TZ,
                '{row['channel']}',
                {row['sequence_num']},
                PARSE_JSON($${json.dumps(row['payload'])}$$)
        """)
    cursor.close()
    print(f"[SNOWFLAKE] Inserted {len(rows)} rows into RAW.{table}")

# Buffer to batch inserts
buffer = []
conn = None

def on_open(ws):
    print(f"[{datetime.now()}] Connected to Coinbase WebSocket")
    
    # Subscribe to market trades
    ws.send(json.dumps({
        "type": "subscribe",
        "product_ids": PRODUCTS,
        "channel": "market_trades"
    }))
    
    # Subscribe to heartbeats
    ws.send(json.dumps({
        "type": "subscribe",
        "product_ids": PRODUCTS,
        "channel": "heartbeats"
    }))
    
    print(f"[{datetime.now()}] Subscribed to market_trades and heartbeats")



def on_message(ws, message):
    global buffer, conn
    data = json.loads(message)
    received_at = datetime.now(timezone.utc).isoformat()
    channel = data.get("channel", "unknown")
    sequence_num = data.get("sequence_num", 0)

    # Save locally as backup
    with open("data/raw_messages.jsonl", "a") as f:
        f.write(json.dumps(data) + "\n")

    # Route heartbeats separately
    if channel == "heartbeats":
        try:
            cursor = conn.cursor()
            for event in data.get("events", []):
                cursor.execute(f"""
                    INSERT INTO RAW.HEARTBEATS 
                    (received_at, product_id, heartbeat_counter, source_ts, raw_payload)
                    SELECT
                        '{received_at}'::TIMESTAMP_TZ,
                        'ALL',
                        {event.get("heartbeat_counter", 0)},
                        '{received_at}'::TIMESTAMP_TZ,
                        PARSE_JSON($${json.dumps(data)}$$)
                """)
            cursor.close()
            print(f"[HEARTBEAT] seq={sequence_num}")
        except Exception as e:
            print(f"[ERROR] Heartbeat write failed: {e}")
        return

    # Add market trades to buffer
    buffer.append({
        "received_at": received_at,
        "channel": channel,
        "sequence_num": sequence_num,
        "payload": data
    })

    print(f"[{received_at}] {channel} seq={sequence_num} buffer={len(buffer)}")

    # Write to Snowflake every 10 messages
    if len(buffer) >= 10:
        try:
            write_to_snowflake(conn, "MARKET_EVENTS", buffer)
            buffer = []
        except Exception as e:
            print(f"[ERROR] Snowflake write failed: {e}")

def on_error(ws, error):
    print(f"[ERROR] {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"[CLOSED] {close_status_code} - {close_msg}")

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    print("Connecting to Snowflake...")
    conn = get_snowflake_connection()
    print("Snowflake connected! Starting WebSocket...")
    ws = websocket.WebSocketApp(
        "wss://advanced-trade-ws.coinbase.com",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()