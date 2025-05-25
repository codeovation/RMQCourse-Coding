// Order Service
package main

import (
	"fmt"
	"log"
	"net/http"
)

func main() {
	log.Println("Order Service running on port 8082...")
	http.HandleFunc("/order", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("User ordered a product")
		w.Write([]byte("User ordered a product"))
	})
	http.ListenAndServe(":8082", nil)
}
