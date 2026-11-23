FROM golang:1.26-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=0
RUN go build -trimpath -o /service ./cmd/api
RUN go build -trimpath -o /worker ./cmd/worker
RUN go build -trimpath -o /maintenance ./cmd/maintenance

FROM alpine:3.23
RUN adduser -D -H app
COPY --from=build /service /worker /maintenance /usr/local/bin/
USER app
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/service"]
