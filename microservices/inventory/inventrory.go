// Inventory Service
package main

import (
	"fmt"
	"log"
	"net/http"
)

func main() {
	log.Println("Inventory Service running on port 8084...")
	http.HandleFunc("/update-stock", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("Stock updated")
		w.Write([]byte("Stock updated"))
	})
	http.ListenAndServe(":8084", nil)
}
