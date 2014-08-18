package demo

import (
	"net/http"
)

func init() {
	http.HandleFunc("/", root)
}

func root(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("hello world"))
}
