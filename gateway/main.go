package main

import (
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
)

// Target servis Python Anda
const (
	SERVICE_B_URL = "http://localhost:8001" // Service B (Cart/Order)
	SERVICE_A_URL = "http://localhost:8002" // Service A (Stock/Master)
)

// NewReverseProxy membuat instance reverse proxy baru
func NewReverseProxy(target string) (*httputil.ReverseProxy, error) {
	targetURL, err := url.Parse(target)
	if err != nil {
		return nil, err
	}
	return httputil.NewSingleHostReverseProxy(targetURL), nil
}

// handleRequest adalah "Resepsionis" utama
func handleRequest(w http.ResponseWriter, r *http.Request) {
	proxyA, err := NewReverseProxy(SERVICE_A_URL)
	if err != nil { log.Fatal(err) }
	proxyB, err := NewReverseProxy(SERVICE_B_URL)
	if err != nil { log.Fatal(err) }

	path := r.URL.Path

	// --- LOGIKA ROUTING YANG BENAR-BENAR FINAL ---

	// 1. Rute-rute milik SERVICE A (Master/Stok)
    // Rute internal baru untuk 'process_stock' juga harus ke A
	if strings.HasPrefix(path, "/cart/add") || 
       strings.HasPrefix(path, "/stock") ||
       strings.HasPrefix(path, "/cart/fallback") ||
       strings.HasPrefix(path, "/order/process_stock") {
		
        log.Printf("Mengarahkan %s ke Service A (Stock)", path)
		proxyA.ServeHTTP(w, r)
		return
	}

	// 2. Rute-rute milik SERVICE B (Replika/Bayar)
    // Rute '/cart' (tanpa /add) dan '/order/pay' ke B
	if strings.HasPrefix(path, "/cart") || strings.HasPrefix(path, "/order") {
		log.Printf("Mengarahkan %s ke Service B (Cart/Order)", path)
		proxyB.ServeHTTP(w, r)
		return
	}

	// Jika tidak ada yang cocok
	log.Printf("Path %s tidak cocok", path)
	http.Error(w, "Not Found", http.StatusNotFound)
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", handleRequest)

	log.Println("Go API Gateway berjalan di port :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}
}