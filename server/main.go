package main

import (
	"os"

	"github.com/alecthomas/kingpin"
	"github.com/gurupras/phonelab_backend"
)

var (
	app            *kingpin.Application
	port           *int
	stagingDirBase *string
	outDirBase     *string
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
	phonelab_backend.Port = *port
	phonelab_backend.StagingDirBase = *stagingDirBase
	phonelab_backend.OutDirBase = *outDirBase
}

func Main(args []string) (err error) {
	parser := setup_parser()
	ParseArgs(parser, args)

	workChannel := make(chan *phonelab_backend.Work, 1000)
	go phonelab_backend.PendingWorkHandler(workChannel)
	server, err := phonelab_backend.SetupServer(phonelab_backend.Port, true, workChannel)
	if err != nil {
		return err
	}
	phonelab_backend.RunServer(server)
	return
}

func main() {
	Main(os.Args)
}
