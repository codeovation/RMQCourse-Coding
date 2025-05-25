// User Service
package main

import (
	"fmt"
	"log"
	"net/http"
)

func main() {
	log.Println("User Service running on port 8081...")
	http.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("User logged in")
		w.Write([]byte("User logged in"))
	})
	http.ListenAndServe(":8081", nil)
}
