package main

import (
	"github.com/let-z-go/gogorpc/cmd/protoc-gen-gogorpc/internal"
)

func main() {
	internal.WriteResponse(internal.GenerateCode(internal.ReadRequest()))
}
