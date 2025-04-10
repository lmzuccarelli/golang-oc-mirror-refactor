.PHONY: all clean test verify build 

build: 
	mkdir -p build
	go build -o build ./... 

build-dev:
	mkdir -p build
	GOOS=linux go build -ldflags="-s -w" -o build -tags real./...

test:
	mkdir -p tests/results
	go test -v -short -coverprofile=tests/results/cover.out ./...

cover:
	go tool cover -html=tests/results/cover.out -o tests/results/cover.html

verify:
	golangci-lint run -c .golangci.yaml 
	# use golangci-lint with only new changes i.e simulate a PR and add files to exclude
	# golangci-lint run -v v2/internal/pkg/release/ --exclude-files "graph*,core*,client*,signature*,find*,new*,local*" --new
	
clean:
	rm -rf build/*
	go clean ./...


