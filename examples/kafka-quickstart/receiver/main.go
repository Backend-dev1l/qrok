// Minimal local receiver: prints every event forwarded by `qrok listen`.
package main

import (
	"io"
	"log"
	"net/http"
)

func main() {
	const addr = ":8888"

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		log.Printf("event id=%s topic=%s key=%q offset=%s replay=%s payload=%s",
			r.Header.Get("X-Qrok-Event-Id"),
			r.Header.Get("X-Qrok-Topic"),
			r.Header.Get("X-Qrok-Key"),
			r.Header.Get("X-Qrok-Offset"),
			r.Header.Get("X-Qrok-Replay"),
			body,
		)
		w.WriteHeader(http.StatusOK)
	})

	log.Printf("receiver listening on %s (waiting for events from qrok listen)", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
