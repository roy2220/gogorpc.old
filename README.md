# pbrpc

Lightweight RPC framework based on Protocol Buffers for Go

## Features

- Full-duplex communication
- Heartbeat detection
- Flow control
- Auto-reconnection
- Service discovery
- Load balancing
- Interception
- Tracing

## Requirements

- go 1.9+
- protobuf 3
- python 2/3

## Installation

```bash
# install library
go get -u -v github.com/let-z-go/pbrpc

# install protoc-gen-gogofaster
go get -u -v github.com/gogo/protobuf/protoc-gen-gogofaster

# install protoc-gen-pbrpc
pip install -U git+https://github.com/let-z-go/protoc-gen-pbrpc
```

## Usage examples

- [sample](./sample)

