FROM golang

WORKDIR /usr/src/franz-go

COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .
RUN go build -v -o /usr/local/bin/kgo-verifier ./...
