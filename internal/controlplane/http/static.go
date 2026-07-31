package httpapi

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed web/*
var dashboardFS embed.FS

func dashboardHandler() http.Handler {
	sub, err := fs.Sub(dashboardFS, "web")
	if err != nil {
		panic("dashboard fs: " + err.Error())
	}
	fileServer := http.FileServer(http.FS(sub))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		switch path {
		case "", "/":
			path = "index.html"
		case "device", "device/":
			path = "device.html"
		}
		r.URL.Path = "/" + path
		fileServer.ServeHTTP(w, r)
	})
}
