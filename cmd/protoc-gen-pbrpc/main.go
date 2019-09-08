package main

import (
	"github.com/let-z-go/pbrpc/cmd/protoc-gen-pbrpc/internal"
)

func main() {
	internal.WriteResponse(internal.GenerateCode(internal.ReadRequest()))
}
