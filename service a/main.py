import os
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Body
from kafka import KafkaConsumer, KafkaProducer
import json
import psycopg2
import threading
import logging
import time
from typing import List, Dict, Any
load_dotenv()

# --- Konfigurasi ---
KAFKA_SERVER = "localhost:9092"
TOPIC_STOCK_SYNC = "stock_sync" 
TOPIC_CART_SYNC = "cart_sync"
TOPIC_ORDER_PAID = "stock_sold" 

DB_CONNECT_STRING = os.getenv("DATABASE_URL")

if not DB_CONNECT_STRING:
    logger.error("FATAL ERROR: DATABASE_URL tidak ditemukan di .env")
    
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("service-a")
app = FastAPI()

# --- (A) Producer & DB Conn ---
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logger.info("[Service A] Kafka Producer Terhubung.")
except Exception as e:
    producer = None
    logger.error(f"[Service A] Kafka Producer Gagal: {e}")

def get_db_connection():
    try:
        conn = psycopg2.connect(DB_CONNECT_STRING)
        return conn
    except Exception as e:
        logger.error(f"[Service A] Gagal koneksi db_a: {e}")
        return None

# --- (B) Logika Consumer (Mendengar 'order_paid') ---

def process_paid_order(data):
    """Logika SETELAH BAYAR: HANYA membersihkan cart."""
    conn = get_db_connection()
    if not conn: return
    
    customer_id = data['customer_id']
    
    try:
        cursor = conn.cursor()
        
        # 1. HAPUS 'cart' di db_a (karena sudah lunas)
        cursor.execute("DELETE FROM cart WHERE user_id = %s", (customer_id,))
        conn.commit()
        logger.info(f"[Service A] Master cart untuk user {customer_id} dibersihkan dari db_a.")
        
        # 2. KIRIM PESAN SINKRONISASI HAPUS CART
        if producer:
            cart_delete_message = {"action": "delete_by_user", "user_id": customer_id}
            producer.send(TOPIC_CART_SYNC, cart_delete_message)
            logger.info(f"[Service A] Menerbitkan (HAPUS CART) ke '{TOPIC_CART_SYNC}': {cart_delete_message}")

    except Exception as e:
        logger.error(f"[Service A] Gagal proses order lunas: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def start_kafka_consumer():
    logger.info("[Service A] Memulai Kafka Consumer thread...")
    try:
        consumer = KafkaConsumer(
            TOPIC_ORDER_PAID,
            bootstrap_servers=KAFKA_SERVER,
            auto_offset_reset='earliest',
            group_id='service-a-group',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        logger.info(f"[Service A] Consumer mendengarkan topic '{TOPIC_ORDER_PAID}'...")
        
        for message in consumer:
            data = message.value
            logger.info(f"[Service A] Menerima pesan '{TOPIC_ORDER_PAID}': {data}")
            if "customer_id" in data:
                process_paid_order(data)
                
    except Exception as e:
        logger.error(f"[Service A] Consumer Kafka error: {e}")
        time.sleep(5)
        start_kafka_consumer()

# --- (C) API Endpoint (Add to Cart) ---
#  INI LOGIKA YANG BENAR (TIDAK MENGURANGI STOK) 
@app.post("/cart/add")
def add_to_cart(data: dict = Body(...)):
    """
    API Utama (MASTER): HANYA cek stok & menambah 'cart' di db_a.
    Stok TIDAK BERKURANG di sini.
    """
    user_id = data.get('user_id')
    stock_id = data.get('stock_id')
    quantity = data.get('quantity')
    
    if not all([user_id, stock_id, quantity]):
        raise HTTPException(status_code=400, detail="Missing data")
    
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=503, detail="Database A error")
        
    try:
        cursor = conn.cursor()
        
        # 1. HANYA CEK STOK (TIDAK MENGURANGI)
        cursor.execute("SELECT current_stock FROM stock WHERE stock_id = %s", (stock_id,))
        stock_result = cursor.fetchone()
        
        # Cek jika stok 0
        if not stock_result or stock_result[0] <= 0:
            raise Exception("Stok 0 atau barang tidak ditemukan")
        
        # 2. Tambah ke 'cart' di db_a (INSERT BIASA)
        sql_insert_cart = "INSERT INTO cart (user_id, stock_id, quantity) VALUES (%s, %s, %s) RETURNING *"
        cursor.execute(sql_insert_cart, (user_id, stock_id, quantity))
        new_cart_item_row = cursor.fetchone()
        
        conn.commit()
        logger.info(f"[Service A] Sukses simpan ke db_a (Master) - BARIS BARU.")
        
        # 3. KIRIM 1 PESAN SINKRONISASI (HANYA CART)
        if producer:
            cart_message = {
                "action": "insert",
                "cart_id": new_cart_item_row[0], 
                "user_id": new_cart_item_row[1],
                "stock_id": new_cart_item_row[2],
                "quantity": new_cart_item_row[3],
                "created_at": new_cart_item_row[4].isoformat()
            }
            producer.send(TOPIC_CART_SYNC, cart_message)
            logger.info(f"[Service A] Menerbitkan ke '{TOPIC_CART_SYNC}': {cart_message}")
            
        return {"status": "success", "message": "Item added to master cart as new row"}
        
    except Exception as e:
        logger.error(f"[Service A] Gagal add to cart: {e}")
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

# --- (D) KUNCI UTAMA: API PENGUNCI STOK (Internal) ---

@app.post("/order/process_stock")
def process_stock(data: dict = Body(...)):
    """
    API INTERNAL (Dipanggil Service B)
    Mencoba mengurangi stok secara atomik.
    """
    items_to_process: List[Dict[str, Any]] = data.get('items', [])
    if not items_to_process:
        raise HTTPException(status_code=400, detail="No items to process")
        
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=503, detail="Database A error")
        
    try:
        cursor = conn.cursor()
        
        for item in items_to_process:
            stock_id = item['stock_id']
            quantity = item['quantity']

            # Ini adalah 'kunci' atomik
            sql_update_stock = """
            UPDATE stock 
            SET current_stock = current_stock - %s 
            WHERE stock_id = %s AND current_stock >= %s
            RETURNING current_stock, product_name
            """
            cursor.execute(sql_update_stock, (quantity, stock_id, quantity))
            
            stock_result = cursor.fetchone()
            if not stock_result:
                # JIKA KOSONG: Berarti UPDATE gagal (stok 0 atau keduluan orang)
                raise Exception(f"Stok untuk {stock_id} tidak cukup (keduluan)")

            # Jika berhasil, kirim pesan sinkronisasi stok
            new_stock_level = stock_result[0]
            product_name = stock_result[1]
            
            if producer:
                stock_message = {
                    "action": "update",
                    "stock_id": stock_id,
                    "new_stock_level": new_stock_level,
                    "product_name": product_name
                }
                producer.send(TOPIC_STOCK_SYNC, stock_message)
                logger.info(f"[Service A] Menerbitkan (STOK BERKURANG) ke '{TOPIC_STOCK_SYNC}': {stock_message}")

        conn.commit() 
        logger.info("[Service A] Stok berhasil diproses/dikunci.")
        return {"status": "success", "message": "Stock processed successfully"}

    except Exception as e:
        logger.error(f"[Service A] Gagal proses stok (atomik): {e}")
        conn.rollback() # Batalkan semua update stok jika 1 saja gagal
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()

# --- (E) Lain-lain ---
@app.on_event("startup")
def on_startup():
    consumer_thread = threading.Thread(target=start_kafka_consumer, daemon=True)
    consumer_thread.start()

@app.get("/cart/fallback/{user_id}")
def get_fallback_cart(user_id: int):
    # ... (kode ini sama) ...
    pass

@app.get("/")
def read_root():
    return {"service": "Service A (Master) V6 - ATOMIK AMAN"}

@app.get("/stock/{stock_id}")
def get_stock(stock_id: int):
    # ... (kode ini sama) ...
    pass