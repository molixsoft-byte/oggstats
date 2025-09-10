rm ../bin/oggstats/oggstats
export CGO_CFLAGS="-D_LARGEFILE64_SOURCE"
go build -ldflags "-s -w -X main.version=V0.1.0 -X 'main.buildDate=$(date)'" -o ../bin/oggstats/oggstats ogg_stats_app.go
ls -l ../bin/oggstats/oggstats