package pbrpc

import (
	"context"
	"errors"
	"fmt"
	"path"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/let-z-go/toolkit/condition"
	"github.com/let-z-go/toolkit/lazy_map"
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
	policy                          *RegistryPolicy
	serviceName2ServiceProviderList lazy_map.LazyMap
	randomizedLoadBalancer          randomizedLoadBalancer
	roundRobinLoadBalancer          roundRobinLoadBalancer
	consistentHashingLoadBalancer   consistentHashingLoadBalancer
	serverAddress2Channel           lazy_map.LazyMap
	lock                            sync.Mutex
	condition                       condition.Condition
	asyncTasks                      []func(context.Context)
	openness                        int32
}

func (self *Registry) Initialize(client *zk.Client, policy *RegistryPolicy) *Registry {
	if self.openness != 0 {
		panic(errors.New("pbrpc: registry already initialized"))
	}

	self.client = client
	self.policy = policy.Validate()
	self.condition.Initialize(&self.lock)
	self.openness = 1
	return self
}

func (self *Registry) Run(context_ context.Context) error {
	if self.openness != 1 {
		return nil
	}

	var wg sync.WaitGroup

	for {
		var e error
		var asyncTasks []func(context.Context)
		self.lock.Lock()

		if self.asyncTasks == nil {
			_, e = self.condition.WaitFor(context_)

			if e != nil {
				atomic.StoreInt32(&self.openness, -1)
			}
		} else {
			e = nil
		}

		asyncTasks = self.asyncTasks
		self.asyncTasks = nil
		self.lock.Unlock()

		for _, asyncTask := range asyncTasks {
			wg.Add(1)

			go func(asyncTask func(context.Context)) {
				asyncTask(context_)
				wg.Done()
			}(asyncTask)
		}

		if e != nil {
			wg.Wait()
			return e
		}
	}
}

func (self *Registry) AddServiceProviders(context_ context.Context, serviceNames []string, serverAddress string, weight int32) error {
	self.checkUninitialized()
	serviceProviderKey := makeServiceProviderKey(serverAddress, weight)

	for i, serviceName := range serviceNames {
		serviceProvidersPath := self.makeServiceProvidersPath(serviceName)

		if e := utils.CreateP(self.client, context_, serviceProvidersPath); e != nil {
			return e
		}

		self.policy.Channel.Logger.Infof("service provider addition: serviceName=%#v, serverAddress=%#v, weight=%#v", serviceName, serverAddress, weight)

		if _, e := self.client.Create(context_, path.Join(serviceProvidersPath, serviceProviderKey), nil, nil, zk.CreateEphemeral, false); e != nil {
			for j := i - 1; j >= 0; j-- {
				serviceName := serviceNames[j]
				serviceProvidersPath := self.makeServiceProvidersPath(serviceName)
				self.policy.Channel.Logger.Infof("service provider removal: serviceName=%#v, serverAddress=%#v, weight=%#v", serviceName, serverAddress, weight)
				self.client.Delete(context_, path.Join(serviceProvidersPath, serviceProviderKey), -1, true)
			}

			return e
		}
	}

	return nil
}

func (self *Registry) RemoveServiceProviders(context_ context.Context, serviceNames []string, serverAddress string, weight int32) error {
	self.checkUninitialized()
	serviceProviderKey := makeServiceProviderKey(serverAddress, weight)
	e := error(nil)

	for _, serviceName := range serviceNames {
		serviceProvidersPath := self.makeServiceProvidersPath(serviceName)
		self.policy.Channel.Logger.Infof("service provider removal: serviceName=%#v, serverAddress=%#v, weight=%#v", serviceName, serverAddress, weight)

		if e2 := self.client.Delete(context_, path.Join(serviceProvidersPath, serviceProviderKey), -1, true); e2 != nil {
			if e3, ok := e2.(*zk.Error); !(ok && e3.GetCode() == zk.ErrorNoNode) {
				e = e2
			}
		}
	}

	return e
}

func (self *Registry) GetMethodCaller(lbType LBType, lbArgument uintptr) MethodCaller {
	var loadBalancer_ loadBalancer

	switch lbType {
	case LBRandomized:
		loadBalancer_ = &self.randomizedLoadBalancer
	case LBRoundRobin:
		loadBalancer_ = &self.roundRobinLoadBalancer
	case LBConsistentHashing:
		loadBalancer_ = &self.consistentHashingLoadBalancer
	default:
		panic(fmt.Errorf("pbrpc: invalid load balancing type: lbType=%#v", lbType))
	}

	return methodCallerProxy{func(context_ context.Context, serviceName string, excludedServerList *markingList) (string, MethodCaller, error) {
		if self.isClosed() {
			return "", nil, RegistryClosedError
		}

		serviceProviderList_, e := self.fetchServiceProviderList(context_, serviceName)

		if e != nil {
			if e2, ok := e.(*zk.Error); ok && e2.GetCode() == zk.ErrorNoNode {
				e = noServerError
			}

			return "", nil, e
		}

		serverAddress, ok := loadBalancer_.selectServer(serviceName, serviceProviderList_, lbArgument, excludedServerList)

		if !ok {
			return "", nil, noServerError
		}

		channel, e := self.fetchChannel(serverAddress)

		if e != nil {
			return "", nil, e
		}

		return serverAddress, channel, nil
	}}
}

func (self *Registry) GetMethodCallerWithoutLB(serverAddress string) MethodCaller {
	return methodCallerProxy{func(_ context.Context, _ string, excludedServerList *markingList) (string, MethodCaller, error) {
		if self.isClosed() {
			return "", nil, RegistryClosedError
		}

		if excludedServerList.markItem(serverAddress) {
			return "", nil, noServerError
		}

		channel, e := self.fetchChannel(serverAddress)

		if e != nil {
			return "", nil, e
		}

		return serverAddress, channel, nil
	}}
}

func (self *Registry) checkUninitialized() {
	if atomic.LoadInt32(&self.openness) == 0 {
		panic(errors.New("pbrpc: registry uninitialized"))
	}
}

func (self *Registry) makeServiceProvidersPath(serviceName string) string {
	return fmt.Sprintf(self.policy.ServiceProvidersPathFormat, serviceName)
}

func (self *Registry) fetchServiceProviderList(context_ context.Context, serviceName string) (serviceProviderList, error) {
	var watcher *zk.Watcher

	value, valueClearer, e := self.serviceName2ServiceProviderList.GetOrSetValue(serviceName, func() (interface{}, error) {
		serviceProvidersPath := self.makeServiceProvidersPath(serviceName)
		var response *zk.GetChildren2Response
		var e error
		response, watcher, e = self.client.GetChildren2W(context_, serviceProvidersPath, true)

		if e != nil {
			return nil, e
		}

		serviceProviderList_ := convertToServiceProviderList(response)
		return serviceProviderList_, nil
	})

	if e != nil {
		return serviceProviderList{}, e
	}

	serviceProviderList_ := value.(serviceProviderList)

	if valueClearer != nil {
		if e := self.postAsyncTask(func(context_ context.Context) {
			select {
			case <-watcher.Event():
			case <-context_.Done():
				watcher.Remove()
			}

			valueClearer()
		}); e != nil {
			watcher.Remove()
			valueClearer()
			return serviceProviderList{}, e
		}
	}

	return serviceProviderList_, nil
}

func (self *Registry) fetchChannel(serverAddress string) (*ClientChannel, error) {
	value, valueClearer, _ := self.serverAddress2Channel.GetOrSetValue(serverAddress, func() (interface{}, error) {
		channel := (&ClientChannel{}).Initialize(self.policy.Channel, []string{serverAddress})
		return channel, nil
	})

	channel := value.(*ClientChannel)

	if valueClearer != nil {
		if e := self.postAsyncTask(func(context_ context.Context) {
			channelListener, _ := channel.AddListener(64)

			go func() {
				for channelState := range channelListener.StateChanges() {
					if channelState == ChannelClosed {
						valueClearer()
						return
					}
				}
			}()

			e := channel.Run(context_)
			self.policy.Channel.Logger.Infof("channel run-out: serverAddress=%#v, e=%q", serverAddress, e)
		}); e != nil {
			valueClearer()
			return nil, e
		}
	}

	return channel, nil
}

func (self *Registry) postAsyncTask(asyncTask func(context.Context)) error {
	if self.isClosed() {
		return RegistryClosedError
	}

	self.lock.Lock()

	if self.isClosed() {
		self.lock.Unlock()
		return RegistryClosedError
	}

	self.asyncTasks = append(self.asyncTasks, asyncTask)
	self.condition.Signal()
	self.lock.Unlock()
	return nil
}

func (self *Registry) isClosed() bool {
	return atomic.LoadInt32(&self.openness) != 1
}

type RegistryPolicy struct {
	ServiceProvidersPathFormat string
	Channel                    *ClientChannelPolicy

	validateOnce sync.Once
}

func (self *RegistryPolicy) Validate() *RegistryPolicy {
	self.validateOnce.Do(func() {
		if self.ServiceProvidersPathFormat == "" {
			self.ServiceProvidersPathFormat = defaultServiceProvidersPathFormat
		}

		if self.Channel == nil {
			self.Channel = &defaultClientChannelPolicy
		}
	})

	return self
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

type methodCallerProxy struct {
	methodCallerFetcher func(context.Context, string, *markingList) (string, MethodCaller, error)
}

func (self methodCallerProxy) CallMethod(
	context_ context.Context,
	serviceName string,
	methodName string,
	methodIndex int32,
	resourceID string,
	extraData map[string][]byte,
	request OutgoingMessage,
	responseType reflect.Type,
	autoRetryMethodCall bool,
) (interface{}, error) {
	var excludedServerList markingList

	for {
		serverAddress, methodCaller, e := self.methodCallerFetcher(context_, serviceName, &excludedServerList)

		if e != nil {
			if e == noServerError {
				e = &Error{code: ErrorNotFound}
			}

			return nil, e
		}

		response, e := methodCaller.CallMethod(context_, serviceName, methodName, methodIndex, resourceID, extraData, request, responseType, autoRetryMethodCall)

		if e != nil {
			if e2, ok := e.(*Error); ok && e2.code == ErrorChannelTimedOut {
				excludedServerList.addItem(serverAddress)
				continue
			}
		}

		return response, e
	}
}

func (self methodCallerProxy) CallMethodWithoutReturn(
	context_ context.Context,
	serviceName string,
	methodName string,
	methodIndex int32,
	resourceID string,
	extraData map[string][]byte,
	request OutgoingMessage,
	responseType reflect.Type,
	autoRetryMethodCall bool,
) error {
	var excludedServerList markingList

	for {
		serverAddress, methodCaller, e := self.methodCallerFetcher(context_, serviceName, &excludedServerList)

		if e != nil {
			if e == noServerError {
				e = &Error{code: ErrorNotFound}
			}

			return e
		}

		e = methodCaller.CallMethodWithoutReturn(context_, serviceName, methodName, methodIndex, resourceID, extraData, request, responseType, autoRetryMethodCall)

		if e != nil {
			if e2, ok := e.(*Error); ok && e2.code == ErrorChannelTimedOut {
				excludedServerList.addItem(serverAddress)
				continue
			}
		}

		return e
	}
}

const defaultServiceProvidersPathFormat = "service_providers/%s"

var defaultClientChannelPolicy ClientChannelPolicy
var noServerError = errors.New("pbrpc: no server")

func makeServiceProviderKey(serverAddress string, weight int32) string {
	return serverAddress + "|" + strconv.Itoa(int(weight))
}

func convertToServiceProviderList(response *zk.GetChildren2Response) serviceProviderList {
	var serviceProviderList_ serviceProviderList

	for _, serviceProviderKey := range response.Children {
		serviceProvider, ok := parseServiceProviderKey(serviceProviderKey)

		if !ok {
			continue
		}

		serviceProviderList_.items = append(serviceProviderList_.items, serviceProvider)
	}

	serviceProviderList_.version = response.Stat.PZxid
	return serviceProviderList_
}

func parseServiceProviderKey(serviceProviderKey string) (serviceProvider, bool) {
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

var RegistryClosedError = errors.New("pbrpc: registry closed")
