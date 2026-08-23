package middleware

import (
	"net"
	"net/http"
	"strings"
)

// RealIP trusts forwarding headers only from a private or loopback proxy.
func RealIP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		proxyIP := net.ParseIP(host)
		if proxyIP != nil && (proxyIP.IsPrivate() || proxyIP.IsLoopback()) {
			clientIP := strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-For"), ",")[0])
			if clientIP == "" {
				clientIP = strings.TrimSpace(r.Header.Get("X-Real-IP"))
			}
			if ip := net.ParseIP(clientIP); ip != nil {
				r.RemoteAddr = ip.String()
			}
		}
		next.ServeHTTP(w, r)
	})
}
