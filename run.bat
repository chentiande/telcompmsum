go build dbbusysum.go conf.go
SET CGO_ENABLED=0
SET GOOS=linux
SET GOARCH=amd64
go build dbbusysum.go conf.go