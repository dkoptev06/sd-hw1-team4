FROM golang:1.26-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /rental ./cmd/rental

FROM alpine:3.23
RUN adduser -D -H app
COPY --from=build /rental /usr/local/bin/rental
USER app
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/rental"]
CMD ["api"]
