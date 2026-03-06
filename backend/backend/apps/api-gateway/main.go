package main

import (
	"fmt"
	"net/http"
)

func healthHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Prexus API is running")
}

func main() {
	http.HandleFunc("/health", healthHandler)

	fmt.Println("Prexus API Gateway running on :8080")
	http.ListenAndServe(":8080", nil)
}
