FROM golang:1.26-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /service ./cmd

FROM alpine:3.23
RUN adduser -D -H app
COPY --from=build /service /usr/local/bin/service
USER app
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/service"]
