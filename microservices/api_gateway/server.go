package main

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
)

// Reverse proxy handler
func reverseProxy(target string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		u, _ := url.Parse(target)
		proxy := httputil.NewSingleHostReverseProxy(u)
		proxy.ServeHTTP(w, r)
	}
}

// Launch a mock microservice on its own port with its own ServeMux
func startMockService(port string, name string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		msg := fmt.Sprintf("[%s Service] Received request: %s\n", name, r.URL.Path)
		log.Print(msg)
		fmt.Fprintf(w, msg)
	})

	server := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	go func() {
		log.Printf("%s Service running on port %s\n", name, port)
		err := server.ListenAndServe()
		if err != nil {
			log.Fatalf("Error starting %s service: %v", name, err)
		}
	}()
}

func main() {
	// Start mock microservices
	startMockService("8081", "Login")
	startMockService("8082", "Order")
	startMockService("8083", "Payment")
	startMockService("8084", "Stock")
	startMockService("8085", "Notification")

	// API Gateway
	log.Println("API Gateway running on port 8080...")

	http.HandleFunc("/login", reverseProxy("http://localhost:8081"))
	http.HandleFunc("/order", reverseProxy("http://localhost:8082"))
	http.HandleFunc("/pay", reverseProxy("http://localhost:8083"))
	http.HandleFunc("/update-stock", reverseProxy("http://localhost:8084"))
	http.HandleFunc("/notify", reverseProxy("http://localhost:8085"))

	log.Fatal(http.ListenAndServe(":8080", nil))
}
