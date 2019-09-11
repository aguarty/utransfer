BIN_PATH?=.bin/utransfer
SOURCE_PATH?=${PWD}

vendors:
	go mod vendor
	go mod download
	
build:
	CGO_ENABLED=0 GOOS=linux go build -mod=vendor -o ${BIN_PATH} cmd/*.go