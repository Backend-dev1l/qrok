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
		case "", "/", "index.html":
			// FileServer сам отдаёт index.html для каталога; явный /index.html
			// Go редиректит на ./ и даёт ERR_TOO_MANY_REDIRECTS на /dashboard/.
			r.URL.Path = "/"
		case "device", "device/":
			r.URL.Path = "/device.html"
		default:
			r.URL.Path = "/" + path
		}
		fileServer.ServeHTTP(w, r)
	})
}
