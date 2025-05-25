// Payment Service
package main

import (
	"fmt"
	"log"
	"net/http"
)

func main() {
	log.Println("Payment Service running on port 8083...")
	http.HandleFunc("/pay", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("Payment processed successfully")
		w.Write([]byte("Payment processed successfully"))
	})
	http.ListenAndServe(":8083", nil)
}
