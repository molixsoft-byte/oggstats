del ..\bin\oggstats\oggstats.exe

env GOOS=windows GOARCH=amd64 go build -o ..\bin\oggstats\oggstats.exe .
copy templates\* ..\bin\oggstats\templates\ /y
rem env GOOS=linux GOARCH=amd64 go build -o oggstats ogg_stats_app.go

dir ..\bin\oggstats\oggstats.exe