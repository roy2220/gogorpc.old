package internal

import (
	"bytes"
	"fmt"
	"go/format"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/protoc-gen-gogo/descriptor"
	plugin "github.com/gogo/protobuf/protoc-gen-gogo/plugin"

	"github.com/let-z-go/gogorpc"
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

func (fs *fileSet) Load(context_ *context, request *plugin.CodeGeneratorRequest) {
	fs.Files = make(map[string]*file, len(request.ProtoFile))
	rawFiles := make(map[string]*descriptor.FileDescriptorProto, len(request.ProtoFile))

	for _, rawFile := range request.ProtoFile {
		file_ := file{
			Name: rawFile.GetName(),
		}

		context_.EnterNode(&file_, func() {
			file_.Load(context_, rawFile)
		})

		fs.Files[file_.Name] = &file_
		rawFiles[file_.Name] = rawFile
	}

	fs.InputFiles = make([]*inputFile, 0, len(request.FileToGenerate))

	for _, inputFileName := range request.FileToGenerate {
		file_ := fs.Files[inputFileName]

		inputFile_ := inputFile{
			file: file_,
		}

		context_.EnterNode(&inputFile_, func() {
			inputFile_.Load(context_, rawFiles[inputFileName])
		})

		fs.InputFiles = append(fs.InputFiles, &inputFile_)
	}
}

func (fs *fileSet) Resolve(context_ *context) {
	for _, file_ := range fs.Files {
		context_.EnterNode(file_, func() {
			file_.Resolve(context_)
		})
	}

	for _, inputFile_ := range fs.InputFiles {
		context_.EnterNode(inputFile_, func() {
			inputFile_.Resolve(context_)
		})
	}
}

func (fs *fileSet) EmitCode(context_ *context, response *plugin.CodeGeneratorResponse) {
	for _, inputFile_ := range fs.InputFiles {
		context_.Code.Reset()

		context_.EnterNode(inputFile_, func() {
			inputFile_.EmitCode(context_)
		})

		outputFileName := strings.TrimSuffix(inputFile_.Name, ".proto") + ".gogorpc.go"
		outputFileContent := context_.Code.String()

		if code, err := format.Source([]byte(outputFileContent)); err == nil {
			outputFileContent = string(code)
		}

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

func (f *file) Load(context_ *context, raw *descriptor.FileDescriptorProto) {
	f.PackageName = raw.GetPackage()
	options := raw.Options

	if options == nil || options.GoPackage == nil {
		f.GoImportPath = filepath.Dir(f.Name)
		_, f.GoPackageName = filepath.Split(f.GoImportPath)

		if f.GoImportPath == "" || f.GoPackageName == "" {
			context_.Fatal("missing option `go_package`")
		}
	} else {
		goPackageOption := *options.GoPackage

		if i := strings.LastIndexByte(goPackageOption, ';'); i >= 0 {
			f.GoImportPath = goPackageOption[:i]
			f.GoPackageName = goPackageOption[i+1:]
		} else {
			f.GoImportPath = goPackageOption
			_, f.GoPackageName = filepath.Split(f.GoImportPath)
		}

		if f.GoImportPath == "" || f.GoPackageName == "" {
			context_.Fatalf("invalid option `go_package`: goPackageOption=%#v", goPackageOption)
		}
	}

	f.Messages = make(map[string]*message, len(raw.MessageType))

	for _, rawMessage := range raw.MessageType {
		message_ := message{}
		message_.Load(rawMessage)
		f.Messages[message_.Name] = &message_
	}
}

func (f *file) Resolve(context_ *context) {
	for _, message_ := range f.Messages {
		context_.EnterNode(message_, func() {
			message_.Resolve(context_)
		})
	}
}

func (f *file) GetNodeName() string {
	return f.Name
}

func (f *file) GetNodeNameDelimiter() string {
	return ":"
}

type message struct {
	Name string

	File *file
}

func (m *message) Load(raw *descriptor.DescriptorProto) {
	m.Name = raw.GetName()
}

func (m *message) Resolve(context_ *context) {
	m.File = context_.Nodes[len(context_.Nodes)-2].(*file)
	context_.AddMessage(m)
}

func (m *message) GetNodeName() string {
	return "<message>:" + m.Name
}

func (m *message) GetNodeNameDelimiter() string {
	return ""
}

type inputFile struct {
	*file

	Errors   []*error1
	Services []*service

	GoImports map[string]string

	goReverseImports map[string]string
}

func (if_ *inputFile) Load(context_ *context, raw *descriptor.FileDescriptorProto) {
	if_.Services = make([]*service, 0, len(raw.Service))

	if raw.Options != nil {
		extension, err := proto.GetExtension(raw.Options, gogorpc.E_Error)

		if err == nil {
			for _, rawError := range extension.([]*gogorpc.Error) {
				error_ := error1{
					Code: rawError.Code,
				}

				if error_.Code == "" {
					context_.Fatal("invalid option `gogorpc.error`: empty `code`")
				}

				context_.EnterNode(&error_, func() {
					error_.Load(context_, rawError)
				})

				if_.Errors = append(if_.Errors, &error_)
			}
		}
	}

	for _, rawService := range raw.Service {
		service_ := service{
			Name: rawService.GetName(),
		}

		context_.EnterNode(&service_, func() {
			service_.Load(context_, rawService)
		})

		if_.Services = append(if_.Services, &service_)
	}
}

func (if_ *inputFile) Resolve(context_ *context) {
	for _, error_ := range if_.Errors {
		context_.EnterNode(error_, func() {
			error_.Resolve(context_)
		})
	}

	for _, service_ := range if_.Services {
		context_.EnterNode(service_, func() {
			service_.Resolve(context_)
		})
	}
}

func (if_ *inputFile) ImportGoPackage(goPackageName string, goImportPath string) string {
	if goImportPath == if_.GoImportPath {
		return ""
	}

	if goImportName, ok := if_.goReverseImports[goImportPath]; ok {
		return goImportName
	}

	goImportName := goPackageName
	_, ok := if_.GoImports[goImportName]

	if ok {
		for n := 2; ; n++ {
			if _, ok := if_.GoImports[goImportName]; !ok {
				break
			}

			goImportName = fmt.Sprintf("%s%d", goPackageName, n)
		}
	} else {
		if if_.GoImports == nil {
			if_.GoImports = map[string]string{}
			if_.goReverseImports = map[string]string{}

			for goImportName, goImportPath := range if_.GoImports {
				if_.goReverseImports[goImportPath] = goImportName
			}
		}
	}

	if_.GoImports[goImportName] = goImportPath
	if_.goReverseImports[goImportPath] = goImportName
	return goImportName
}

func (if_ *inputFile) EmitCode(context_ *context) {
	fmt.Fprintf(&context_.Code, `// Code generated by protoc-gen-gogorpc. DO NOT EDIT.

package %s
`, if_.GoPackageName)

	if len(if_.Services) == 0 {
		return
	}

	if err := template.Must(template.New("").Parse(`
import (
{{- range $goImportName, $goImportPath := .GoImports}}
	{{printf "%s %q" $goImportName $goImportPath}}
{{- end}}
)
`)).Execute(&context_.Code, if_); err != nil {
		panic(err)
	}

	if len(if_.Errors) >= 1 {
		if err := template.Must(template.New("").Parse(`
var (
{{- range .Errors}}
	RPCErr{{.Code}} = channel.NewRPCError(channel.RPCErrorType({{.Type}}), "{{.FullCode}}")
{{- end}}
)
`)).Execute(&context_.Code, if_); err != nil {
			panic(err)
		}
	}

	for _, service_ := range if_.Services {
		context_.EnterNode(service_, func() {
			service_.EmitCode(context_)
		})
	}
}

func (if_ *inputFile) GetNodeName() string {
	return if_.Name
}

func (if_ *inputFile) GetNodeNameDelimiter() string {
	return ":"
}

type error1 struct {
	Code string
	Type int32

	InputFile *inputFile
	FullCode  string
}

func (e *error1) Load(context_ *context, raw *gogorpc.Error) {
	if raw.Type == 0 {
		context_.Fatal("invalid option `gogorpc.error`: zero `type`")
	}

	e.Type = raw.Type
}

func (e *error1) Resolve(context_ *context) {
	e.InputFile = context_.Nodes[len(context_.Nodes)-2].(*inputFile)
	e.FullCode = e.InputFile.PackageName + "." + e.Code
	context_.AddError(e)
	e.InputFile.ImportGoPackage("channel", "github.com/let-z-go/gogorpc/channel")
}

func (e *error1) GetNodeName() string {
	return "<gogorpc.error>:" + e.Code
}

func (e *error1) GetNodeNameDelimiter() string {
	return ""
}

type service struct {
	Name string

	Methods []*method

	FullName string
}

func (s *service) Load(context_ *context, raw *descriptor.ServiceDescriptorProto) {
	s.Methods = make([]*method, 0, len(raw.Method))

	for _, rawMethod := range raw.Method {
		method_ := method{
			Name: rawMethod.GetName(),
		}

		context_.EnterNode(&method_, func() {
			method_.Load(context_, rawMethod)
		})

		s.Methods = append(s.Methods, &method_)
	}
}

func (s *service) Resolve(context_ *context) {
	inputFile_ := context_.Nodes[len(context_.Nodes)-2].(*inputFile)
	s.FullName = inputFile_.PackageName + "." + s.Name
	inputFile_.ImportGoPackage("channel", "github.com/let-z-go/gogorpc/channel")

	for _, method_ := range s.Methods {
		context_.EnterNode(method_, func() {
			method_.Resolve(context_)
		})
	}
}

func (s *service) EmitCode(context_ *context) {
	if err := template.Must(template.New("").Parse(`
const Service{{.Name}} = "{{.FullName}}"
{{- if .Methods}}

const (
	{{- range .Methods}}
	{{$.Name}}_{{.Name}} = "{{.Name}}"
	{{- end}}
)
{{- end}}

type {{.Name}} interface {
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

func Implement{{.Name}}(service {{.Name}}) func(*channel.Options) {
	return func(options *channel.Options) {
{{- if .Methods}}
		options.
{{- end}}
{{- range $i, $_ := .Methods}}
			{{- if $i}}
				{{- "."}}
			{{- end}}
			BuildMethod(Service{{$.Name}}, {{$.Name}}_{{.Name}}).
	{{- if .Request}}
			SetRequestFactory({{$.Name}}_New{{.Name}}Request).
	{{- end}}
			SetIncomingRPCHandler(func(rpc *channel.RPC) {
	{{- if .Response}}
				response, err := service.{{.Name}}(rpc.Ctx
		{{- if .Request}}
		{{- ", rpc.Request.(*"}}{{.Request.GoMessagePath}})
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
				rpc.Err = service.{{.Name}}(rpc.Ctx
		{{- if .Request}}
		{{- ", rpc.Request.(*"}}{{.Request.GoMessagePath}})
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
	requestExtraData channel.ExtraData
}

var _ = {{.Name}}({{.Name}}Stub{})

func (ss *{{.Name}}Stub) Init(rpcPreparer channel.RPCPreparer) *{{.Name}}Stub {
	ss.rpcPreparer = rpcPreparer
	return ss
}

func (ss *{{.Name}}Stub) WithRequestExtraData(extraData channel.ExtraData) *{{.Name}}Stub {
	ss.requestExtraData = extraData
	return ss
}
{{- range .Methods}}

func (ss {{$.Name}}Stub) {{.Name}}(ctx context.Context
	{{- if .Request}}
		{{- ", request *"}}{{.Request.GoMessagePath}}
	{{- end}}
	{{- ") "}}
	{{- if .Response}}
		{{- "(*"}}{{.Response.GoMessagePath}}, error)
	{{- else}}
		{{- "error"}}
	{{- end}}
	{{- " {"}}
	rpc := ss.Make{{.Name}}RPC(ctx
	{{- if .Request}}
		{{- ", request"}}
	{{- end}}
	{{- ").Do()"}}
	{{- if .Response}}
	response, err := rpc.Result()
	rpc.Close()
	return response, err
	{{- else}}
	err := rpc.Result()
	rpc.Close()
	return err
	{{- end}}
}

func (ss {{$.Name}}Stub) Make{{.Name}}RPC(ctx context.Context
	{{- if .Request}}
		{{- ", request *"}}{{.Request.GoMessagePath}}
	{{- end}}
	{{- ") "}}{{$.Name}}_{{.Name}}RPC {
	rpc := channel.GetPooledRPC()

	*rpc = channel.RPC{
		Ctx: ctx,
		ServiceName: Service{{$.Name}},
		MethodName: {{$.Name}}_{{.Name}},
		RequestExtraData: ss.requestExtraData.Ref(true),
	{{- if .Request}}
		Request: request,
	{{- end}}
	}

	ss.rpcPreparer.PrepareRPC(rpc,{{" "}}
	{{- if .Response}}
		{{- $.Name}}_New{{.Name}}Response
	{{- else}}
		{{- "channel.GetNullMessage"}}
	{{- end}}
	{{- ")"}}
	return {{$.Name}}_{{.Name}}RPC{rpc}
}
{{- end}}
{{- range .Methods}}

type {{$.Name}}_{{.Name}}RPC struct {
	underlying *channel.RPC
}

func (mr {{$.Name}}_{{.Name}}RPC) WithRequestExtraData(extraData channel.ExtraDataRef) {{$.Name}}_{{.Name}}RPC {
	mr.underlying.RequestExtraData = extraData
	return mr
}

func (mr {{$.Name}}_{{.Name}}RPC) Do() {{$.Name}}_{{.Name}}RPC {
	if mr.underlying.IsHandled() {
		mr.underlying.Reprepare()
	}

	mr.underlying.Handle()
	return mr
}

func (mr {{$.Name}}_{{.Name}}RPC) Result(){{" "}}
	{{- if .Response}}
		{{- "(*"}}{{.Response.GoMessagePath}}, error)
	{{- else}}
		{{- "error"}}
	{{- end}}
	{{- " {"}}
	{{- if .Response}}
	if mr.underlying.Err != nil {
		return nil, mr.underlying.Err
	}

	return mr.underlying.Response.(*{{.Response.GoMessagePath}}), nil
	{{- else}}
	return mr.underlying.Err
	{{- end}}
}

func (mr {{$.Name}}_{{.Name}}RPC) Close() {
	channel.PutPooledRPC(mr.underlying)
	mr.underlying = nil
}

func (mr {{$.Name}}_{{.Name}}RPC) RequestExtraData() channel.ExtraDataRef {
	return mr.underlying.RequestExtraData
}

func (mr {{$.Name}}_{{.Name}}RPC) ResponseExtraData() channel.ExtraDataRef {
	return mr.underlying.ResponseExtraData
}
{{- end}}
{{- range .Methods}}
{{- if .Request}}

func {{$.Name}}_New{{.Name}}Request() channel.Message {
	return new({{.Request.GoMessagePath}})
}
{{- end}}
{{- if .Response}}

func {{$.Name}}_New{{.Name}}Response() channel.Message {
	return new({{.Response.GoMessagePath}})
}
{{- end}}
{{- end}}
`)).Execute(&context_.Code, s); err != nil {
		panic(err)
	}
}

func (s *service) GetNodeName() string {
	return "<service>:" + s.Name
}

func (s *service) GetNodeNameDelimiter() string {
	return "."
}

type method struct {
	Name string

	Request  *reqresp
	Response *reqresp
}

func (m *method) Load(context_ *context, raw *descriptor.MethodDescriptorProto) {
	request := reqresp{}
	request.Load(raw.GetInputType())
	m.Request = &request
	response := reqresp{}
	response.Load(raw.GetOutputType())
	m.Response = &response
}

func (m *method) Resolve(context_ *context) {
	inputFile_ := context_.Nodes[len(context_.Nodes)-3].(*inputFile)
	inputFile_.ImportGoPackage("context", "context")

	switch packageName, messageName := m.Request.PackageName, m.Request.MessageName; {
	case packageName == "gogorpc" && messageName == "Void":
		m.Request = nil
	default:
		file_ := context_.Packages[packageName].Messages[messageName].File
		goImportName := inputFile_.ImportGoPackage(file_.GoPackageName, file_.GoImportPath)

		if goImportName == "" {
			m.Request.GoMessagePath = messageName
		} else {
			m.Request.GoMessagePath = goImportName + "." + messageName
		}
	}

	switch packageName, messageName := m.Response.PackageName, m.Response.MessageName; {
	case packageName == "gogorpc" && messageName == "Void":
		m.Response = nil
	default:
		file_ := context_.Packages[packageName].Messages[messageName].File
		goImportName := inputFile_.ImportGoPackage(file_.GoPackageName, file_.GoImportPath)

		if goImportName == "" {
			m.Response.GoMessagePath = messageName
		} else {
			m.Response.GoMessagePath = goImportName + "." + messageName
		}
	}
}

func (m *method) GetNodeName() string {
	return m.Name
}

func (m *method) GetNodeNameDelimiter() string {
	return ""
}

type reqresp struct {
	PackageName string
	MessageName string

	GoMessagePath string
}

func (r *reqresp) Load(raw string) {
	if i := strings.LastIndexByte(raw, '.'); i >= 1 {
		r.PackageName = raw[1:i]
		r.MessageName = raw[i+1:]
	} else {
		r.MessageName = raw[1:]
	}
}

type context struct {
	Nodes []node

	Packages map[string]*package1

	Code bytes.Buffer
}

func (c *context) EnterNode(node_ node, callback func()) {
	c.Nodes = append(c.Nodes, node_)
	callback()
	c.Nodes = c.Nodes[:len(c.Nodes)-1]
}

func (c *context) AddMessage(message_ *message) {
	package_ := c.getOrSetPackage(message_.File.PackageName)
	package_.Messages[message_.Name] = message_
}

func (c *context) AddError(error_ *error1) {
	package_ := c.getOrSetPackage(error_.InputFile.PackageName)

	if prevError, ok := package_.Errors[error_.Code]; ok {
		c.Fatalf("redefinition: prevFileName=%#v", prevError.InputFile.Name)
	}

	package_.Errors[error_.Code] = error_
}

func (c *context) Fatal(message string) {
	if n := len(c.Nodes); n >= 1 {
		buffer := bytes.Buffer{}

		for _, node := range c.Nodes[:n-1] {
			buffer.WriteString(node.GetNodeName())
			buffer.WriteString(node.GetNodeNameDelimiter())
		}

		buffer.WriteString(c.Nodes[n-1].GetNodeName())
		buffer.WriteString(": ")
		buffer.WriteString(message)
		message = buffer.String()
	}

	panic(exception(message))
}

func (c *context) Fatalf(format string, args ...interface{}) {
	c.Fatal(fmt.Sprintf(format, args...))
}

func (c *context) getOrSetPackage(packageName string) *package1 {
	package_, ok := c.Packages[packageName]

	if !ok {
		if c.Packages == nil {
			c.Packages = map[string]*package1{}
		}

		package_ = &package1{
			Name:     packageName,
			Messages: map[string]*message{},
			Errors:   map[string]*error1{},
		}

		c.Packages[packageName] = package_
	}

	return package_
}

type node interface {
	GetNodeName() string
	GetNodeNameDelimiter() string
}

type package1 struct {
	Name     string
	Messages map[string]*message
	Errors   map[string]*error1
}

type exception string
