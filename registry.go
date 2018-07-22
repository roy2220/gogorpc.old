package pbrpc

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/let-z-go/zk"
	"github.com/let-z-go/zk/recipes/utils"
)

const (
	LBRandomized LBType = iota
	LBRoundRobin
	LBConsistentHashing
)

type Registry struct {
	client                          *zk.Client
	channelPolicy                   *ChannelPolicy
	serviceName2ServiceProviderList sync.Map
	randomizedLoadBalancer          randomizedLoadBalancer
	roundRobinLoadBalancer          roundRobinLoadBalancer
	consistentHashingLoadBalancer   consistentHashingLoadBalancer
	serverAddress2Channel           sync.Map
	context                         context.Context
	exit                            context.CancelFunc
}

func (self *Registry) Initialize(client *zk.Client, channelPolicy *ChannelPolicy, context_ context.Context) *Registry {
	if self.context != nil {
		panic(errors.New("pbrpc: registry already initialized"))
	}

	self.client = client
	self.channelPolicy = channelPolicy

	if context_ == nil {
		context_ = context.Background()
	}

	self.context, self.exit = context.WithCancel(context_)
	return self
}

func (self *Registry) Exit() {
	self.checkUninitialized()
	self.exit()
}

func (self *Registry) AddServiceProviders(serviceNames []string, serverAddress string, weight int32) error {
	self.checkUninitialized()

	for _, serviceName := range serviceNames {
		serviceProvidersPath := makeServiceProvidersPath(serviceName)

		if e := utils.CreateP(self.client, self.context, serviceProvidersPath); e != nil {
			return e
		}

		serviceProviderKey := makeServiceProviderKey(serverAddress, weight)

		if _, e := self.client.Create(self.context, serviceProvidersPath+"/"+serviceProviderKey, []byte{}, nil, zk.CreateEphemeral, true); e != nil {
			if e2, ok := e.(zk.Error); !(ok && e2.GetCode() == zk.ErrorNodeExists) {
				return e
			}
		}
	}

	return nil
}

func (self *Registry) RemoveServiceProviders(serviceNames []string, serverAddress string, weight int32) error {
	self.checkUninitialized()

	for _, serviceName := range serviceNames {
		serviceProvidersPath := makeServiceProvidersPath(serviceName)
		serviceProviderKey := makeServiceProviderKey(serverAddress, weight)

		if e := self.client.Delete(self.context, serviceProvidersPath+"/"+serviceProviderKey, -1, true); e != nil {
			if e2, ok := e.(zk.Error); !(ok && e2.GetCode() == zk.ErrorNoNode) {
				return e
			}
		}
	}

	return nil
}

func (self *Registry) FetchMethodCaller(lbType LBType, lbArgument uintptr) MethodCaller {
	self.checkUninitialized()
	var loadBalancer_ loadBalancer

	switch lbType {
	case LBRandomized:
		loadBalancer_ = &self.randomizedLoadBalancer
	case LBRoundRobin:
		loadBalancer_ = &self.roundRobinLoadBalancer
	case LBConsistentHashing:
		loadBalancer_ = &self.consistentHashingLoadBalancer
	default:
		panic(fmt.Errorf("pbrpc: method caller fetching: lbType=%#v", lbType))
	}

	return dynamicMethodCaller{func(serviceName string, excludedServerList *markingList) (string, MethodCaller, error) {
		serviceProviderList_, e := self.fetchServiceProviderList(serviceName)

		if e != nil {
			if e2, ok := e.(zk.Error); ok && e2.GetCode() == zk.ErrorNoNode {
				e = noServerError
			}

			return "", nil, e
		}

		serverAddress, ok := loadBalancer_.selectServer(serviceName, serviceProviderList_, lbArgument, excludedServerList)

		if !ok {
			return "", nil, noServerError
		}

		channel := self.fetchChannel(serverAddress)
		return serverAddress, channel, nil
	}}
}

func (self *Registry) FetchMethodCallerWithoutLB(serverAddress string) MethodCaller {
	self.checkUninitialized()

	return dynamicMethodCaller{func(_ string, excludedServerList *markingList) (string, MethodCaller, error) {
		if excludedServerList.markItem(serverAddress) {
			return "", nil, noServerError
		}

		channel := self.fetchChannel(serverAddress)
		return serverAddress, channel, nil
	}}
}

func (self *Registry) checkUninitialized() {
	if self.context == nil {
		panic(errors.New("pbrpc: registry uninitialized"))
	}
}

func (self *Registry) fetchServiceProviderList(serviceName string) (serviceProviderList, error) {
retry1:
	value, _ := self.serviceName2ServiceProviderList.LoadOrStore(serviceName, &sync.Mutex{})

	if lock, ok := value.(*sync.Mutex); ok {
	retry2:
		lock.Lock()
		value2, ok := self.serviceName2ServiceProviderList.Load(serviceName)

		if !ok {
			lock.Unlock()
			goto retry1
		}

		if lock2, ok := value2.(*sync.Mutex); ok {
			if lock2 != lock {
				lock.Unlock()
				lock = lock2
				goto retry2
			}

			serviceProvidersPath := makeServiceProvidersPath(serviceName)
			response, watcher, e := self.client.GetChildren2W(self.context, serviceProvidersPath, true)

			if e != nil {
				lock.Unlock()
				return serviceProviderList{}, e
			}

			serviceProviderList_ := parseServiceProviderList(response)
			self.serviceName2ServiceProviderList.Store(serviceName, serviceProviderList_)
			lock.Unlock()

			go func() {
				select {
				case <-watcher.Event():
				case <-self.context.Done():
					watcher.Remove()
				}

				self.serviceName2ServiceProviderList.Delete(serviceName)
			}()

			return serviceProviderList_, nil
		}

		value = value2
		lock.Unlock()
	}

	serviceProviderList_ := value.(serviceProviderList)
	return serviceProviderList_, nil
}

func (self *Registry) fetchChannel(serverAddress string) *ClientChannel {
retry1:
	value, _ := self.serverAddress2Channel.LoadOrStore(serverAddress, &sync.Mutex{})

	if lock, ok := value.(*sync.Mutex); ok {
	retry2:
		lock.Lock()
		value2, ok := self.serverAddress2Channel.Load(serverAddress)

		if !ok {
			lock.Unlock()
			goto retry1
		}

		if lock2, ok := value2.(*sync.Mutex); ok {
			if lock2 != lock {
				lock.Unlock()
				lock = lock2
				goto retry2
			}

			channel := (&ClientChannel{}).Initialize(self.channelPolicy, []string{serverAddress}, self.context)
			self.serverAddress2Channel.Store(serverAddress, channel)
			lock.Unlock()

			go func() {
				channelListener, _ := channel.AddListener(64)

				go func() {
					e := channel.Run()
					self.channelPolicy.Logger.Infof("channel run-out: serverAddress=%#v, e=%#v", serverAddress, e.Error())
				}()

				for channelState := range channelListener.StateChanges() {
					if channelState == ChannelClosed {
						break
					}
				}

				self.serverAddress2Channel.Delete(serverAddress)
			}()

			return channel
		}

		value = value2
		lock.Unlock()
	}

	channel := value.(*ClientChannel)
	return channel
}

type LBType uint8

func (self LBType) GoString() string {
	switch self {
	case LBRandomized:
		return "<LBRandomized>"
	case LBRoundRobin:
		return "<LBRoundRobin>"
	case LBConsistentHashing:
		return "<LBConsistentHashing>"
	default:
		return fmt.Sprintf("<LBType:%d>", self)
	}
}

type dynamicMethodCaller struct {
	fetcher func(string, *markingList) (string, MethodCaller, error)
}

func (self dynamicMethodCaller) CallMethod(context_ context.Context, serviceName string, methodName string, request OutgoingMessage, responseType reflect.Type, autoRetryMethodCall bool) (IncomingMessage, error) {
	var excludedServerList markingList

	for {
		serverAddress, methodCaller, e := self.fetcher(serviceName, &excludedServerList)

		if e != nil {
			if e == noServerError {
				e = Error{true, ErrorNotImplemented, fmt.Sprintf("methodID=%v, request=%#v", representMethodID(serviceName, methodName), request)}
			}

			return nil, e
		}

		response, e := methodCaller.CallMethod(context_, serviceName, methodName, request, responseType, autoRetryMethodCall)

		if e != nil {
			if e, ok := e.(Error); ok && e.code == ErrorChannelTimedOut {
				excludedServerList.addItem(serverAddress)
				continue
			}
		}

		return response, e
	}
}

func (self dynamicMethodCaller) CallMethodWithoutReturn(context_ context.Context, serviceName string, methodName string, request OutgoingMessage, responseType reflect.Type, autoRetryMethodCall bool) error {
	var excludedServerList markingList

	for {
		serverAddress, methodCaller, e := self.fetcher(serviceName, &excludedServerList)

		if e != nil {
			if e == noServerError {
				e = Error{true, ErrorNotImplemented, fmt.Sprintf("methodID=%v, request=%#v", representMethodID(serviceName, methodName), request)}
			}

			return e
		}

		e = methodCaller.CallMethodWithoutReturn(context_, serviceName, methodName, request, responseType, autoRetryMethodCall)

		if e != nil {
			if e, ok := e.(Error); ok && e.code == ErrorChannelTimedOut {
				excludedServerList.addItem(serverAddress)
				continue
			}
		}

		return e
	}
}

var noServerError = errors.New("pbrpc: no server")

func makeServiceProvidersPath(serviceName string) string {
	return "service_providers/" + serviceName
}

func makeServiceProviderKey(serverAddress string, weight int32) string {
	return serverAddress + "|" + strconv.Itoa(int(weight))
}

func parseServiceProviderList(response *zk.GetChildren2Response) serviceProviderList {
	var serviceProviderList_ serviceProviderList

	for _, serviceProviderKey := range response.Children {
		serviceProvider, ok := parseServiceProvider(serviceProviderKey)

		if !ok {
			continue
		}

		serviceProviderList_.items = append(serviceProviderList_.items, serviceProvider)
	}

	serviceProviderList_.version = response.Stat.PZxid
	return serviceProviderList_
}

func parseServiceProvider(serviceProviderKey string) (serviceProvider, bool) {
	i := strings.IndexByte(serviceProviderKey, '|')

	if i < 0 {
		return serviceProvider{}, false
	}

	serverAddress := serviceProviderKey[:i]
	weight, e := strconv.ParseUint(serviceProviderKey[i+1:], 10, 31)

	if e != nil || weight == 0 {
		return serviceProvider{}, false
	}

	var serviceProvider_ serviceProvider
	serviceProvider_.serverAddress = serverAddress
	serviceProvider_.weight = int32(weight)
	return serviceProvider_, true
}
