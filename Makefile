proto:
	go install github.com/gogo/protobuf/protoc-gen-gogofaster
	PATH=${PATH}:${GOPATH}/bin protoc --proto_path="${GOPATH}/src" --gogofaster_out=Mgoogle/protobuf/descriptor.proto=github.com/gogo/protobuf/protoc-gen-gogo/descriptor:"${GOPATH}/src" github.com/let-z-go/gogorpc/gogorpc.proto
	PATH=${PATH}:${GOPATH}/bin protoc --proto_path="${GOPATH}/src" --gogofaster_out="${GOPATH}/src" github.com/let-z-go/gogorpc/internal/proto/uuid.proto
	PATH=${PATH}:${GOPATH}/bin protoc --proto_path="${GOPATH}/src" --gogofaster_out="${GOPATH}/src" github.com/let-z-go/gogorpc/internal/proto/transport.proto
	PATH=${PATH}:${GOPATH}/bin protoc --proto_path="${GOPATH}/src" --gogofaster_out="${GOPATH}/src" github.com/let-z-go/gogorpc/internal/proto/stream.proto

.PHONY: proto
