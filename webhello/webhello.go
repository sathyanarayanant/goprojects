package main

import (
	"fmt"
	"net/http"
)

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("inside handler")
	fmt.Fprintf(w, "hello web world!")
}

func main() {
	http.HandleFunc("/", handler)
	port := ":8080"
	fmt.Println("Listening in port", port)
	http.ListenAndServe(port, nil)
}
