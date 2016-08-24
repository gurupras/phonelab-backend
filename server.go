package phonelab_backend

import (
	"fmt"

	"github.com/alecthomas/kingpin"
	"github.com/labstack/echo"
	"github.com/labstack/echo/engine/fasthttp"
	"github.com/labstack/echo/middleware"
)

type Work struct {
	FilePath    string
	Version     string
	DeviceId    string
	PackageName string
	FileName    string
}

var (
	app            *kingpin.Application
	port           *int
	stagingDirBase *string
	outDirBase     *string

	Port           int
	StagingDirBase string
	OutDirBase     string
)

func setup_parser() *kingpin.Application {
	app = kingpin.New("phonelab-backend-server", "")
	port = app.Flag("port", "Port to run webserver on").Default("8081").Int()
	stagingDirBase = app.Flag("stage-dir", "Directory in which to stage files for processing").Required().String()
	outDirBase = app.Flag("out", "Directory in which to store processed files").Required().String()
	return app
}

func ParseArgs(parser *kingpin.Application, args []string) {
	kingpin.MustParse(parser.Parse(args[1:]))

	// Now for the conversions
	Port = *port
	StagingDirBase = *stagingDirBase
	OutDirBase = *outDirBase
}

func RunServer(port int) (err error) {
	server := echo.New()

	server.Use(middleware.Logger())
	//server.Use(middleware.Gzip())

	// Set up the routes
	server.POST("/uploader/:version/:deviceId/:packageName/:fileName", HandleUploaderPost)
	server.GET("/uploader/:version/:deviceId/:packageName/:fileName", HandleUploaderGet)

	go PendingWorkHandler()
	// Start the server
	server.Run(fasthttp.New(fmt.Sprintf(":%d", Port)))
	return
}

func Main(args []string) {
	parser := setup_parser()
	ParseArgs(parser, args)

	RunServer(Port)
}
