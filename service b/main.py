import os
from dotenv import load_dotenv # <-- Import
from fastapi import FastAPI, HTTPException, Body
from kafka import KafkaConsumer, KafkaProducer
import json
import psycopg2
import threading
import logging # <-- Import
import time
import httpx # Butuh httpx untuk panggil Service A
from typing import List, Dict, Any

# Panggil fungsi ini TEPAT SETELAH import
load_dotenv() # <-- BUG 1: Tambahkan tanda kurung ()

# --- Konfigurasi ---
KAFKA_SERVER = "localhost:9092"
TOPIC_STOCK_SYNC = "stock_sync" 
TOPIC_CART_SYNC = "cart_sync"
TOPIC_ORDER_PAID = "stock_sold"
SERVICE_A_URL = "http://localhost:8002" # Alamat Service A (Internal)

# --- INI BLOK YANG HILANG (BUG 2) ---
# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("service-b")
# ------------------------------------

# GANTI DENGAN INI:
DB_CONNECT_STRING = os.getenv("DATABASE_URL")

if not DB_CONNECT_STRING:
    logger.error("FATAL ERROR: DATABASE_URL tidak ditemukan di .env")
    # Anda bisa exit di sini
    
app = FastAPI()

# --- (A) Producer & DB Conn ---
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logger.info("[Service B] Kafka Producer Terhubung.")
except Exception as e:
    producer = None
    logger.error(f"[Service B] Kafka Producer Gagal: {e}")

def get_db_connection():
    try:
        conn = psycopg2.connect(DB_CONNECT_STRING)
        return conn
    except Exception as e:
        logger.error(f"[Service B] Gagal koneksi db_b: {e}")
        return None

# --- (B) Logika Consumer (Sinkronisasi Replika) ---
# (Fungsi 'sync_database' ini SAMA SEKALI TIDAK BERUBAH)
def sync_database(data, topic):
    """Meniru semua perubahan dari Service A ke db_b (Replika)."""
    conn = get_db_connection()
    if not conn: return
    try:
        cursor = conn.cursor()
        
        if topic == TOPIC_STOCK_SYNC and data['action'] == 'update':
            sql = """
            INSERT INTO stock (stock_id, current_stock, product_name) 
            VALUES (%s, %s, %s)
            ON CONFLICT (stock_id) 
            DO UPDATE SET 
                current_stock = EXCLUDED.current_stock,
                product_name = EXCLUDED.product_name;
            """
            cursor.execute(sql, (
                data['stock_id'], 
                data['new_stock_level'], 
                data.get('product_name', 'N/A')
            ))
            logger.info(f"[Service B] Sinkronisasi (UPSERT) STOK: {data['stock_id']}")
            
        elif topic == TOPIC_CART_SYNC:
            if data['action'] == 'insert':
                sql = """
                INSERT INTO cart (cart_id, user_id, stock_id, quantity, created_at) 
                VALUES (%s, %s, %s, %s, %s) 
                ON CONFLICT (cart_id) DO NOTHING 
                """
                cursor.execute(sql, (data['cart_id'], data['user_id'], data['stock_id'], data['quantity'], data['created_at']))
                logger.info(f"[Service B] Sinkronisasi CART (Replika) INSERT: {data['cart_id']}")
            
            elif data['action'] == 'delete_by_user':
                sql = "DELETE FROM cart WHERE user_id = %s"
                cursor.execute(sql, (data['user_id'],))
                logger.info(f"[Service B] Sinkronisasi CART (Replika) DELETE: user {data['user_id']}")
        
        conn.commit()
    except Exception as e:
        logger.error(f"[Service B] Gagal sinkronisasi db_b: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def start_kafka_consumer():
    logger.info("[Service B] Memulai Kafka Consumer thread...")
    try:
        consumer = KafkaConsumer(
            TOPIC_STOCK_SYNC,
            TOPIC_CART_SYNC,
            bootstrap_servers=KAFKA_SERVER,
            auto_offset_reset='earliest',
            group_id='service-b-group',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        logger.info(f"[Service B] Consumer mendengarkan 2 topic...")
        
        for message in consumer:
            data = message.value
            topic = message.topic
            logger.info(f"[Service B] Menerima pesan dari topic '{topic}': {data}")
            sync_database(data, topic)
                
    except Exception as e:
        logger.error(f"[Service B] Consumer Kafka error: {e}")
        time.sleep(5)
        start_kafka_consumer()

# --- (C) API Endpoint (Bayar) ---
#  INI FUNGSI 'pay_order' YANG BENAR-BENAR AMAN 
@app.post("/order/pay")
async def pay_order(data: dict = Body(...)):
    """
    API Pembayaran:
    1. Baca cart dari db_b (Replika)
    2. PANGGIL Service A untuk KUNCI STOK (Atomik)
    3. Simpan ke db_b.orders
    4. Terbitkan 'order_paid'
    """
    customer_id = data.get('customer_id')
    if not customer_id:
        raise HTTPException(status_code=400, detail="Missing customer_id")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=503, detail="Database B error")

    items_in_cart: List[tuple] = []
    items_to_process: List[Dict[str, Any]] = []
    
    try:
        cursor = conn.cursor()
        
        # 1. BACA SEMUA BARANG di keranjang dari db_b (Replika)
        cursor.execute("SELECT cart_id, stock_id, quantity FROM cart WHERE user_id = %s", (customer_id,))
        items_in_cart = cursor.fetchall() 
        
        if not items_in_cart:
            raise HTTPException(status_code=404, detail="Keranjang kosong")

        # Siapkan data untuk dikirim ke Service A
        for item in items_in_cart:
            items_to_process.append({"stock_id": item[1], "quantity": item[2]})

        # 2. PANGGIL Service A untuk KUNCI STOK (SINKRON)
        logger.info(f"[Service B] Mencoba mengunci stok ke Service A untuk {customer_id}...")
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    f"{SERVICE_A_URL}/order/process_stock", # Panggil API internal baru
                    json={"items": items_to_process}
                )
            
            # Jika Service A bilang stok habis (keduluan), GAGALKAN
            if response.status_code != 200:
                logger.warning(f"Gagal kunci stok di Service A: {response.text}")
                raise HTTPException(status_code=400, detail=f"Stok habis atau keduluan: {response.json().get('detail')}")
        
        except httpx.RequestError as e:
            logger.error(f"Gagal menghubungi Service A: {e}")
            raise HTTPException(status_code=503, detail="Service A (Stock) tidak terjangkau")

        
        # 3. STOK AMAN! Lanjutkan proses bayar & simpan 'orders'
        logger.info(f"[Service B] Stok berhasil dikunci. Menyimpan {len(items_in_cart)} item ke orders.")
        
        items_to_publish = []
        for item in items_in_cart:
            cart_id, stock_id, quantity = item
            sql_order = "INSERT INTO orders (customer_id, stock_id, quantity, status) VALUES (%s, %s, %s, %s)"
            cursor.execute(sql_order, (customer_id, stock_id, quantity, 'PAID'))
            items_to_publish.append({"stock_id": stock_id, "quantity": quantity})
        
        conn.commit() # Simpan semua baris 'orders'
        logger.info(f"[Service B] Order berhasil disimpan ke db_b.orders untuk user {customer_id}.")
        
        # 4. Terbitkan pesan bahwa pembayaran LUNAS
        if producer:
            message = {"customer_id": customer_id, "items": items_to_publish}
            producer.send(TOPIC_ORDER_PAID, message)
            logger.info(f"[Service B] Menerbitkan pesan ke '{TOPIC_ORDER_PAID}': {message}")
            
        return {"status": "success", "message": "Pembayaran berhasil!"}
        
    except HTTPException as he:
        conn.rollback()
        raise he # Lemparkan error HTTP
    except Exception as e:
        logger.error(f"[Service B] Gagal proses bayar: {e}")
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()
    
# --- (D) Lain-lain ---
@app.on_event("startup")
def on_startup():
    consumer_thread = threading.Thread(target=start_kafka_consumer, daemon=True)
    consumer_thread.start()

@app.get("/")
def read_root():
    return {"service": "Service B (Replica) V6 - ATOMIK AMAN"}

@app.get("/cart/{user_id}")
def get_cart(user_id: int):
    # ... (kode ini sama) ...
    pass