[![Build Status](https://travis-ci.org/gurupras/phonelab_backend.svg?branch=master)](https://travis-ci.org/gurupras/phonelab_backend) 
[![Codecov branch](https://img.shields.io/codecov/c/github/gurupras/phonelab_backend/master.svg?maxAge=2592000?style=plastic)]()

# phonelab-backend
Phonelab's backend log processing written in Golang


# Run
To run the HTTP server, run `go build` inside the `server` directory and run this binary.

    server --stage-dir=STAGE-DIR --out=OUT [<flags>]

    Flags:
      --help                 Show context-sensitive help (also try --help-long and --help-man).
      --port=8081            Port to run webserver on
      --stage-dir=STAGE-DIR  Directory in which to stage files for processing
      --out=OUT              Directory in which to store processed files

# Webserver Configuration
The server is based on [echo](https://github.com/labstack/echo). If you would like to deploy the server behind `NGINX`, add the following to your `site.conf` file.

    location /phonelab {
        rewrite /phonelab(.*) $1 break;
        proxy_pass_header Server;
        proxy_set_header Host $http_host;
        proxy_redirect off;
        proxy_http_version 1.1;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Scheme $scheme;
        proxy_pass http://127.0.0.1:8081/;
    }

This configuration assumes that the `conductor`'s upload URL has the following scheme: `http://example.com/phonelab/`.
The full upload URL would thus resolve into:

    http://example.com/phonelab/uploader/:version/:deviceid/:packagename/:filename

The above `NGINX` configuration strips out the `/phonelab` and sends the remainder as the resource request.
