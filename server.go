package phonelab_backend

import (
	"fmt"
	"os"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/gurupras/go-stoppableNetListener"
	"github.com/labstack/echo"
	"github.com/labstack/echo/engine"
	"github.com/labstack/echo/engine/fasthttp"
	"github.com/labstack/echo/middleware"
)

var (
	logger *logrus.Logger
)

func InitLogger(loggers ...*logrus.Logger) {
	if loggers != nil && len(loggers) != 0 {
		logger = loggers[0]
	} else {
		// By default, we're at panic level
		logger = logrus.New()
		logger.Level = logrus.PanicLevel
	}
}

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

func SetupServer(port int, config *Config, useLogger bool) (server *Server, err error) {
	if server, err = New(port); err != nil {
		return
	}

	if useLogger {
		server.Use(middleware.Logger())
	}
	//server.Use(middleware.Gzip())

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
