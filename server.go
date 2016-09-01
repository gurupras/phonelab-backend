package phonelab_backend

import (
	"fmt"
	"os"
	"sync"

	"github.com/gurupras/go-stoppableNetListener"
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
	serverWg := sync.WaitGroup{}
	serverWg.Add(1)
	go func() {
		defer serverWg.Done()
		// Recover from echo panic
		defer func() {
			if r := recover(); r != nil {
				fmt.Fprintln(os.Stderr, r)
			}
		}()
		s.Echo.Run(fasthttp.WithConfig(config))
	}()
	serverWg.Wait()
}

func (s *Server) Stop() {
	s.snl.Stop()
}

var (
	Port           int
	stagingDirBase string
	outDirBase     string
)

func addRoutes(server *Server) {
}

func SetupServer(port int, config *Config, useLogger bool) (server *Server, err error) {
	if server, err = New(port); err != nil {
		return
	}

	if useLogger {
		server.Use(middleware.Logger())
	}

	if config.WorkChannel == nil {
		config.WorkChannel = make(chan *Work, 1000)
	}

	if config.StagingConfig == nil {
		config.StagingConfig = InitializeStagingConfig()
	}

	if config.ProcessingConfig == nil {
		config.ProcessingConfig = InitializeProcessingConfig()
	}

	// Set up the routes
	handleUploaderPost := func(c echo.Context) error {
		return HandleUploaderPost(c, config)
	}
	server.POST("/uploader/:version/:deviceId/:packageName/:fileName", handleUploaderPost)
	return server, err
}

func RunServer(server *Server) {
	// Start the server
	server.Run()
}
