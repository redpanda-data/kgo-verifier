
all: build

build: build-verifier build-repeater

build-verifier:
	go build -o kgo-verifier cmd/kgo-verifier/main.go

build-repeater:
	go build -o kgo-repeater cmd/kgo-repeater/main.go
