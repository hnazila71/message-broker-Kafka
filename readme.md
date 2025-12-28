# Microservices Kafka Message Broker

Proyek ini adalah implementasi arsitektur **Microservices** dengan **Apache Kafka** sebagai Message Broker untuk komunikasi asinkron antar layanan. Sistem ini terdiri dari API Gateway (Go) yang menerima permintaan HTTP dan meneruskannya ke layanan Python melalui topik Kafka.

## Daftar Isi
- [Arsitektur](#arsitektur)
- [Prasyarat](#prasyarat)
- [Struktur Proyek](#struktur-proyek)
- [Instalasi dan Menjalankan](#instalasi-dan-menjalankan)
- [Pengujian API](#pengujian-api)
- [Routing Gateway](#routing-gateway)

## Arsitektur

### Alur Data Sistem:
1. **Gateway (Go)**: Bertindak sebagai API Gateway yang menerima request HTTP dan mengarahkannya ke service yang tepat
2. **Service A (Python FastAPI)**: Melayani endpoint terkait stock dan master data
3. **Service B (Python FastAPI)**: Melayani endpoint terkait cart dan order

### Port Configuration:
| Service | Port | URL |
|---------|------|-----|
| API Gateway | 8080 | http://localhost:8080 |
| Service A | 8002 | http://localhost:8002 |
| Service B | 8001 | http://localhost:8001 |

## Prasyarat

Pastikan sudah terinstal:
* [Go](https://go.dev/dl/) versi 1.18+
* [Python](https://www.python.org/downloads/) versi 3.8+
* [Docker & Docker Compose](https://www.docker.com/)

## Struktur Proyek

```
├── gateway/      # API Gateway (Go - Port 8080)
├── service_a/    # Service A (Python FastAPI - Port 8002)
└── service_b/    # Service B (Python FastAPI - Port 8001)
```

## Instalasi dan Menjalankan

### 1. Menjalankan Gateway
```bash
cd gateway
go mod tidy
go run main.go
```
Gateway berjalan di: http://localhost:8080

### 2. Menjalankan Service A
```bash
cd service_a
pip install -r requirements.txt
python main.py
```
Service A berjalan di: http://localhost:8002

### 3. Menjalankan Service B
```bash
cd service_b
pip install -r requirements.txt
python main.py
```
Service B berjalan di: http://localhost:8001

## Pengujian API

### Endpoints Service A (Stock/Master):
```bash
# Stock endpoints
GET http://localhost:8080/stock
POST http://localhost:8080/stock/update

# Cart fallback
POST http://localhost:8080/cart/fallback

# Process stock
POST http://localhost:8080/order/process_stock
```

### Endpoints Service B (Cart/Order):
```bash
# Cart endpoints (kecuali /cart/add)
GET http://localhost:8080/cart
DELETE http://localhost:8080/cart/{item_id}

# Order endpoints
GET http://localhost:8080/order
POST http://localhost:8080/order/pay
```

### Test dengan curl:
```bash
# Test Service A
curl -X GET http://localhost:8080/stock

# Test Service B
curl -X GET http://localhost:8080/cart

# Test cart/add (ini akan diarahkan ke Service A)
curl -X POST http://localhost:8080/cart/add \
  -H "Content-Type: application/json" \
  -d '{"product_id": 1, "quantity": 2}'
```

## Routing Gateway

### Logika Routing:

#### **Diarahkan ke Service A (Port 8002):**
- `/cart/add` - Menambahkan item ke cart
- `/stock` - Semua endpoint stock (GET, POST, PUT, DELETE)
- `/cart/fallback` - Fallback mechanism untuk cart
- `/order/process_stock` - Proses stock untuk order

#### **Diarahkan ke Service B (Port 8001):**
- `/cart` (tanpa `/add`) - Semua endpoint cart lainnya
- `/order` - Semua endpoint order (termasuk `/order/pay`)

### Contoh Routing:
```
Request: GET /cart → Service B (8001)
Request: POST /cart/add → Service A (8002)
Request: GET /stock → Service A (8002)
Request: POST /order/pay → Service B (8001)
```

### Fitur Gateway:
- **Reverse Proxy**: Mengarahkan request ke service yang sesuai
- **Logging**: Mencatat semua request yang masuk
- **Load Balancing**: Siap untuk di-extend dengan multiple instances
- **Error Handling**: Mengembalikan 404 untuk path yang tidak dikenal