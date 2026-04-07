package http

import (
	"io/fs"
	"mime"
	"net/http"
	"path"
	"strings"

	embeddedui "shore-master/monitor/http/ui"

	"github.com/gin-gonic/gin"
)

func (s *Server) registerUIRoutes() error {
	distFS, err := embeddedui.DistFS()
	if err != nil {
		return err
	}

	if !uiFileExists(distFS, "index.html") {
		return fs.ErrNotExist
	}

	s.engine.NoRoute(func(c *gin.Context) {
		if c.Request.Method != http.MethodGet && c.Request.Method != http.MethodHead {
			c.Status(http.StatusNotFound)
			return
		}

		requestPath := normalizedUIPath(c.Request.URL.Path)
		if strings.HasPrefix(requestPath, "api/") || strings.HasPrefix(requestPath, "ws/") {
			c.Status(http.StatusNotFound)
			return
		}

		if requestPath == "" || path.Ext(requestPath) == "" {
			serveUIFile(c, distFS, "index.html")
			return
		}

		if !uiFileExists(distFS, requestPath) {
			c.Status(http.StatusNotFound)
			return
		}

		serveUIFile(c, distFS, requestPath)
	})

	return nil
}

func normalizedUIPath(rawPath string) string {
	cleanPath := path.Clean("/" + rawPath)
	if cleanPath == "/" || cleanPath == "." {
		return ""
	}

	return strings.TrimPrefix(cleanPath, "/")
}

func uiFileExists(distFS fs.FS, name string) bool {
	_, err := fs.Stat(distFS, name)
	return err == nil
}

func serveUIFile(c *gin.Context, distFS fs.FS, name string) {
	content, err := fs.ReadFile(distFS, name)
	if err != nil {
		c.Status(http.StatusNotFound)
		return
	}

	contentType := mime.TypeByExtension(path.Ext(name))
	if contentType == "" {
		contentType = http.DetectContentType(content)
	}

	c.Data(http.StatusOK, contentType, content)
}
