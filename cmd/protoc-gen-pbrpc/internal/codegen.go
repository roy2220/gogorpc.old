package internal

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	plugin "github.com/golang/protobuf/protoc-gen-go/plugin"
)

func ReadRequest() *plugin.CodeGeneratorRequest {
	data, err := ioutil.ReadAll(os.Stdin)

	if err != nil {
		panic(err)
	}

	request := plugin.CodeGeneratorRequest{}

	if err := proto.Unmarshal(data, &request); err != nil {
		panic(err)
	}

	return &request
}

func WriteResponse(response *plugin.CodeGeneratorResponse) {
	data, err := proto.Marshal(response)

	if err != nil {
		panic(err)
	}

	if _, err := os.Stdout.Write(data); err != nil {
		panic(err)
	}
}

func GenerateCode(request *plugin.CodeGeneratorRequest) (response *plugin.CodeGeneratorResponse) {
	response = &plugin.CodeGeneratorResponse{}

	defer func() {
		if x := recover(); x != nil {
			if exception_, ok := x.(exception); ok {
				response.Error = (*string)(&exception_)
			} else {
				panic(x)
			}
		}
	}()

	fileSet_ := fileSet{}
	context_ := context{}
	fileSet_.Load(&context_, request)
	fileSet_.Resolve(&context_)
	fileSet_.EmitCode(&context_, response)
	return
}

type fileSet struct {
	Files      map[string]*file
	InputFiles []*inputFile
}

func (self *fileSet) Load(context_ *context, request *plugin.CodeGeneratorRequest) {
	self.Files = make(map[string]*file, len(request.ProtoFile))
	rawFiles := make(map[string]*descriptor.FileDescriptorProto, len(request.ProtoFile))

	for _, rawFile := range request.ProtoFile {
		file_ := file{
			Name: rawFile.GetName(),
		}

		context_.EnterNode(&file_, func() {
			file_.Load(context_, rawFile)
		})

		self.Files[file_.Name] = &file_
		rawFiles[file_.Name] = rawFile
	}

	self.InputFiles = make([]*inputFile, 0, len(request.FileToGenerate))

	for _, inputFileName := range request.FileToGenerate {
		file_ := self.Files[inputFileName]

		inputFile_ := inputFile{
			file: file_,
		}

		context_.EnterNode(&inputFile_, func() {
			inputFile_.Load(context_, rawFiles[inputFileName])
		})

		self.InputFiles = append(self.InputFiles, &inputFile_)
	}
}

func (self *fileSet) Resolve(context_ *context) {
	for _, file_ := range self.Files {
		context_.EnterNode(file_, func() {
			file_.Resolve(context_)
		})
	}

	for _, inputFile_ := range self.InputFiles {
		context_.EnterNode(inputFile_, func() {
			inputFile_.Resolve(context_)
		})
	}
}

func (self *fileSet) EmitCode(context_ *context, response *plugin.CodeGeneratorResponse) {
	for _, inputFile_ := range self.InputFiles {
		context_.Code.Reset()

		context_.EnterNode(inputFile_, func() {
			inputFile_.EmitCode(context_)
		})

		outputFileName := strings.TrimSuffix(inputFile_.Name, ".proto") + ".pbrpc.go"
		outputFileContent := context_.Code.String()

		response.File = append(response.File, &plugin.CodeGeneratorResponse_File{
			Name:    &outputFileName,
			Content: &outputFileContent,
		})
	}
}

type file struct {
	Name string

	PackageName   string
	GoImportPath  string
	GoPackageName string
	Messages      map[string]*message
}

func (self *file) Load(context_ *context, raw *descriptor.FileDescriptorProto) {
	self.PackageName = raw.GetPackage()
	options := raw.Options

	if options == nil || options.GoPackage == nil {
		self.GoImportPath = filepath.Dir(self.Name)
		_, self.GoPackageName = filepath.Split(self.GoImportPath)

		if self.GoImportPath == "" || self.GoPackageName == "" {
			context_.Fatal("missing `go_package` option")
		}
	} else {
		goPackageOption := *options.GoPackage

		if i := strings.LastIndexByte(goPackageOption, ';'); i < 0 {
			self.GoImportPath = goPackageOption
			_, self.GoPackageName = filepath.Split(self.GoImportPath)
		} else {
			self.GoImportPath = goPackageOption[:i]
			self.GoPackageName = goPackageOption[i+1:]
		}

		if self.GoImportPath == "" || self.GoPackageName == "" {
			context_.Fatal(fmt.Sprintf("invalid `go_package` option: %#v", goPackageOption))
		}
	}

	self.Messages = make(map[string]*message, len(raw.MessageType))

	for _, rawMessage := range raw.MessageType {
		message_ := message{}
		message_.Load(rawMessage)
		self.Messages[message_.Name] = &message_
	}
}

func (self *file) Resolve(context_ *context) {
	for _, message_ := range self.Messages {
		message_.File = self
		context_.AddMessage(message_)
	}
}

func (self *file) GetName() string {
	return self.Name
}

func (self *file) GetNameDelimiter() string {
	return ":"
}

type message struct {
	Name string

	File *file
}

func (self *message) Load(raw *descriptor.DescriptorProto) {
	self.Name = raw.GetName()
}

type inputFile struct {
	*file

	Services []*service

	GoImports map[string]string

	goReverseImports map[string]string
}

func (self *inputFile) Load(context_ *context, raw *descriptor.FileDescriptorProto) {
	self.Services = make([]*service, 0, len(raw.Service))

	for _, rawService := range raw.Service {
		service_ := service{
			Name: rawService.GetName(),
		}

		context_.EnterNode(&service_, func() {
			service_.Load(context_, rawService)
		})

		self.Services = append(self.Services, &service_)
	}
}

func (self *inputFile) Resolve(context_ *context) {
	for _, service_ := range self.Services {
		context_.EnterNode(service_, func() {
			service_.Resolve(context_)
		})
	}
}

func (self *inputFile) ImportGoPackage(goPackageName string, goImportPath string) string {
	if goImportPath == self.GoImportPath {
		return ""
	}

	if goImportName, ok := self.goReverseImports[goImportPath]; ok {
		return goImportName
	}

	goImportName := goPackageName
	_, ok := self.GoImports[goImportName]

	if ok {
		for n := 2; ; n++ {
			if _, ok := self.GoImports[goImportName]; !ok {
				break
			}

			goImportName = fmt.Sprintf("%s%d", goPackageName, n)
		}
	} else {
		if self.GoImports == nil {
			self.GoImports = map[string]string{}
			self.goReverseImports = map[string]string{}

			for goImportName, goImportPath := range self.GoImports {
				self.goReverseImports[goImportPath] = goImportName
			}
		}
	}

	self.GoImports[goImportName] = goImportPath
	self.goReverseImports[goImportPath] = goImportName
	return goImportName
}

func (self *inputFile) EmitCode(context_ *context) {
	fmt.Fprintf(&context_.Code, `/*
 * Generated by Aspector. DO NOT EDIT!
 */

package %s
`, self.GoPackageName)

	if len(self.Services) == 0 {
		return
	}

	if err := template.Must(template.New("").Parse(`
import (
{{- range $goImportName, $goImportPath := .GoImports}}
	{{printf "%s %q" $goImportName $goImportPath}}
{{- end}}
)
`)).Execute(&context_.Code, self); err != nil {
		panic(err)
	}

	for _, service_ := range self.Services {
		context_.EnterNode(service_, func() {
			service_.EmitCode(context_)
		})
	}
}

func (self *inputFile) GetName() string {
	return self.Name
}

func (self *inputFile) GetNameDelimiter() string {
	return ":"
}

type service struct {
	Name string

	Methods []*method
}

func (self *service) Load(context_ *context, raw *descriptor.ServiceDescriptorProto) {
	self.Methods = make([]*method, 0, len(raw.Method))

	for _, rawMethod := range raw.Method {
		method_ := method{
			Name: rawMethod.GetName(),
		}

		context_.EnterNode(&method_, func() {
			method_.Load(context_, rawMethod)
		})

		self.Methods = append(self.Methods, &method_)
	}
}

func (self *service) Resolve(context_ *context) {
	inputFile_ := context_.Nodes[len(context_.Nodes)-2].(*inputFile)
	inputFile_.ImportGoPackage("channel", "github.com/let-z-go/pbrpc/channel")

	for _, method_ := range self.Methods {
		context_.EnterNode(method_, func() {
			method_.Resolve(context_)
		})
	}
}

func (self *service) EmitCode(context_ *context) {
	if err := template.Must(template.New("").Parse(`
const {{.Name}} = "{{.Name}}"
{{- if .Methods}}

const (
	{{- range .Methods}}
	{{$.Name}}_{{.Name}} = "{{.Name}}"
	{{- end}}
)
{{- end}}

type {{.Name}}Handler interface {
{{- range .Methods}}
	{{.Name}}(ctx context.Context
	{{- if .Request}}
		{{- ", request *"}}{{.Request.GoMessagePath}}
	{{- end}}
	{{- ") ("}}
	{{- if .Response}}
		{{- "response *"}}{{.Response.GoMessagePath}},{{" "}}
	{{- end}}
	{{- "err error)"}}
{{- end}}
}

func Register{{.Name}}Handler(serviceHandler {{.Name}}Handler) func(*channel.Options) {
	return func(options *channel.Options) {
{{- if .Methods}}
		options.
{{- end}}
{{- range $i, $_ := .Methods}}
			{{- if $i}}
				{{- "."}}
			{{- end}}
			BuildMethod({{$.Name}}, {{$.Name}}_{{.Name}}).
	{{- if .Request}}
			SetRequestFactory(func() channel.Message {
				return new({{.Request.GoMessagePath}})
			}).
	{{- end}}
			SetIncomingRPCHandler(func(rpc *channel.RPC) {
	{{- if .Response}}
				response, err := serviceHandler.{{.Name}}(rpc.Ctx
		{{- if .Request}}
		{{- ", "}}rpc.Request.(*{{.Request.GoMessagePath}})
		{{- end}}
		{{- ")"}}

				if response == nil {
					rpc.Response = channel.NullMessage
				} else {
					rpc.Response = response
				}

				rpc.Err = err
	{{- else}}
				rpc.Response = channel.NullMessage
				rpc.Err = serviceHandler.{{.Name}}(rpc.Ctx
		{{- if .Request}}
		{{- ", "}}rpc.Request.(*{{.Request.GoMessagePath}})
		{{- end}}
		{{- ")"}}
	{{- end}}
			}).
			End()
{{- end}}
	}
}

type {{.Name}}Stub struct {
	rpcPreparer channel.RPCPreparer
}
{{- range .Methods}}

func (self {{$.Name}}Stub) {{.Name}}(ctx context.Context
	{{- if .Request}}
		{{- ", request *"}}{{.Request.GoMessagePath}}
	{{- end}}
	{{- ") *"}}{{$.Name}}Stub_{{.Name}} {
	rpc := {{$.Name}}Stub_{{.Name}}{inner: channel.RPC{
		Ctx: ctx,
		ServiceName: {{$.Name}},
		MethodName: {{$.Name}}_{{.Name}},
	{{- if .Request}}
		Request: request,
	{{- end}}
	}}

	self.rpcPreparer.PrepareRPC(&rpc.inner,{{" "}}
	{{- if .Response}}
		{{- "func() channel.Message {"}}
		return new({{.Response.GoMessagePath}})
	})
{{""}}
	{{- else}}
		{{- "channel.NewNullMessage"}})
	{{- end}}
	return &rpc
}
{{- end}}

func Make{{.Name}}Stub(rpcPreparer channel.RPCPreparer) {{.Name}}Stub {
	return {{.Name}}Stub{rpcPreparer}
}
{{- range .Methods}}

type {{$.Name}}Stub_{{.Name}} struct {
	inner channel.RPC
}

func (self *{{$.Name}}Stub_{{.Name}}) WithRequestMetadata(metadata channel.Metadata) *{{$.Name}}Stub_{{.Name}} {
	self.inner.RequestMetadata = metadata
	return self
}

func (self *{{$.Name}}Stub_{{.Name}}) Invoke(){{" "}}
	{{- if .Response}}
		{{- "(*"}}{{.Response.GoMessagePath}}, error)
	{{- else}}
		{{- "error"}}
	{{- end}}
	{{- " {"}}
	if self.inner.IsHandled() {
		self.inner.Reprepare()
	}

	self.inner.Handle()
	{{- if .Response}}

	if self.inner.Err != nil {
		return nil, self.inner.Err
	}

	return self.inner.Response.(*{{.Response.GoMessagePath}}), nil
	{{- else}}
	return self.inner.Err
	{{- end}}
}

func (self *{{$.Name}}Stub_{{.Name}}) ResponseMetadata() channel.Metadata {
	return self.inner.ResponseMetadata
}
{{- end}}
`)).Execute(&context_.Code, self); err != nil {
		panic(err)
	}
}

func (self *service) GetName() string {
	return self.Name
}

func (self *service) GetNameDelimiter() string {
	return "."
}

type method struct {
	Name string

	Request  *reqresp
	Response *reqresp
}

func (self *method) Load(context_ *context, raw *descriptor.MethodDescriptorProto) {
	request := reqresp{}
	request.Load(raw.GetInputType())
	self.Request = &request
	response := reqresp{}
	response.Load(raw.GetOutputType())
	self.Response = &response
}

func (self *method) Resolve(context_ *context) {
	inputFile_ := context_.Nodes[len(context_.Nodes)-3].(*inputFile)
	inputFile_.ImportGoPackage("context", "context")

	switch packageName, messageName := self.Request.PackageName, self.Request.MessageName; {
	case packageName == "pbrpc" && messageName == "Void":
		self.Request = nil
	default:
		file_ := context_.Packages[packageName].Messages[messageName].File
		goImportName := inputFile_.ImportGoPackage(file_.GoPackageName, file_.GoImportPath)

		if goImportName == "" {
			self.Request.GoMessagePath = messageName
		} else {
			self.Request.GoMessagePath = goImportName + "." + messageName
		}
	}

	switch packageName, messageName := self.Response.PackageName, self.Response.MessageName; {
	case packageName == "pbrpc" && messageName == "Void":
		self.Response = nil
	default:
		file_ := context_.Packages[packageName].Messages[messageName].File
		goImportName := inputFile_.ImportGoPackage(file_.GoPackageName, file_.GoImportPath)

		if goImportName == "" {
			self.Response.GoMessagePath = messageName
		} else {
			self.Response.GoMessagePath = goImportName + "." + messageName
		}
	}
}

func (self *method) GetName() string {
	return self.Name
}

func (self *method) GetNameDelimiter() string {
	return ""
}

type reqresp struct {
	PackageName string
	MessageName string

	GoMessagePath string
}

func (self *reqresp) Load(raw string) {
	i := strings.LastIndexByte(raw, '.')

	if i == 0 {
		self.MessageName = raw[1:]
	} else {
		self.PackageName = raw[1:i]
		self.MessageName = raw[i+1:]
	}
}

type context struct {
	Nodes []node

	Packages map[string]*package1

	Code bytes.Buffer
}

func (self *context) EnterNode(node_ node, callback func()) {
	self.Nodes = append(self.Nodes, node_)
	callback()
	self.Nodes = self.Nodes[:len(self.Nodes)-1]
}

func (self *context) AddMessage(message_ *message) {
	package_, ok := self.Packages[message_.File.PackageName]

	if !ok {
		if self.Packages == nil {
			self.Packages = map[string]*package1{}
		}

		package_ = &package1{
			Name:     message_.File.PackageName,
			Messages: map[string]*message{},
		}

		self.Packages[message_.File.PackageName] = package_
	}

	package_.Messages[message_.Name] = message_
}

func (self *context) Fatal(message string) {
	if n := len(self.Nodes); n >= 1 {
		buffer := bytes.Buffer{}

		for _, node := range self.Nodes[:n-1] {
			buffer.WriteString(node.GetName())
			buffer.WriteString(node.GetNameDelimiter())
		}

		buffer.WriteString(self.Nodes[n-1].GetName())
		buffer.WriteString(": ")
		buffer.WriteString(message)
		message = buffer.String()
	}

	panic(exception(message))
}

type node interface {
	GetName() string
	GetNameDelimiter() string
}

type package1 struct {
	Name     string
	Messages map[string]*message
}

type exception string
