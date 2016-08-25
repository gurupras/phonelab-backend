package phonelab_backend

import (
	"github.com/gurupras/stoppableNetListener"
	"github.com/labstack/echo"
	"github.com/labstack/echo/engine"
	"github.com/labstack/echo/engine/fasthttp"
	"github.com/labstack/echo/middleware"
)

type Server struct {
	*echo.Echo
	snl *stoppableNetListener.StoppableNetListener
}

func New(port int) (server *Server, err error) {
	var snl *stoppableNetListener.StoppableNetListener
	if snl, err = stoppableNetListener.New(port); err != nil {
		return
	}
	server = new(Server)
	server.Echo = echo.New()
	server.snl = snl
	return
}

func (s *Server) Run() {
	config := engine.Config{}
	config.Listener = s.snl
	s.Echo.Run(fasthttp.WithConfig(config))
}

func (s *Server) Stop() {
	s.snl.Stop()
}

var (
	Port           int
	StagingDirBase string
	OutDirBase     string
)

func addRoutes(server *Server) {
	server.POST("/uploader/:version/:deviceId/:packageName/:fileName", HandleUploaderPost)
	server.GET("/uploader/:version/:deviceId/:packageName/:fileName", HandleUploaderGet)
}

func SetupServer(port int, useLogger bool) (server *Server, err error) {
	if server, err = New(port); err != nil {
		return
	}

	if useLogger {
		server.Use(middleware.Logger())
	}

	// Set up the routes
	addRoutes(server)
	return server, err
}

func RunServer(server *Server) {
	// Start the server
	server.Run()
}
