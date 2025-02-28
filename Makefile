.PHONY: all test build clean


build: 
	mkdir -p build
	go build -o build ./... 

clean:
	rm -rf build/*
	go clean ./...

