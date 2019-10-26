.PHONY: all
all: proto vet lint test

.PHONY: proto
proto:
	PATH=${PATH}:${GOPATH}/bin protoc --proto_path="${GOPATH}/src" --gogofaster_out=Mgoogle/protobuf/descriptor.proto=github.com/gogo/protobuf/protoc-gen-gogo/descriptor:"${GOPATH}/src" github.com/let-z-go/gogorpc/gogorpc.proto
	PATH=${PATH}:${GOPATH}/bin protoc --proto_path="${GOPATH}/src" --gogofaster_out="${GOPATH}/src" github.com/let-z-go/gogorpc/internal/proto/uuid.proto
	PATH=${PATH}:${GOPATH}/bin protoc --proto_path="${GOPATH}/src" --gogofaster_out="${GOPATH}/src" github.com/let-z-go/gogorpc/internal/proto/transport.proto
	PATH=${PATH}:${GOPATH}/bin protoc --proto_path="${GOPATH}/src" --gogofaster_out="${GOPATH}/src" github.com/let-z-go/gogorpc/internal/proto/stream.proto

.PHONY: vet
vet:
	go vet ./...

.PHONY: lint
lint:
	./scripts/golint.sh ./...

.PHONY: test
test:
	go test ./...
