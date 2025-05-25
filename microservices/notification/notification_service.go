// Notification Service
package main

import (
	"fmt"
	"log"
	"net/http"
)

func main() {
	log.Println("Notification Service running on port 8085...")
	http.HandleFunc("/notify", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("User notified")
		w.Write([]byte("User notified"))
	})
	http.ListenAndServe(":8085", nil)
}
