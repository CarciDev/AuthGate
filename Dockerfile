FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /authgate ./cmd/authgate

FROM gcr.io/distroless/static-debian12
COPY --from=build /authgate /authgate
ENTRYPOINT ["/authgate"]
