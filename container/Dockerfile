FROM golang:1.22-alpine AS build
WORKDIR /app
COPY go.mod ./
COPY main.go ./
RUN go build -o /zipper main.go

FROM alpine:3.20
WORKDIR /app
COPY --from=build /zipper /usr/local/bin/zipper
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/zipper"]

