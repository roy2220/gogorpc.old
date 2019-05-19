package pbrpc

import (
	"hash/fnv"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"sync"
)

type loadBalancer interface {
	selectServer(serviceName string, serviceProviderList_ serviceProviderList, lbArgument uintptr, excludedServerList *markingList) (serverAddress string, ok bool)
}

type serviceProviderList struct {
	items   []serviceProvider
	version int64
}

type serviceProvider struct {
	serverAddress string
	weight        int32
}

type randomizedState struct {
	lock                   sync.Mutex
	serviceProviderList    serviceProviderList
	serviceProviderIndexes []int
}

type randomizedLoadBalancer struct {
	serviceName2State sync.Map
}

func (self *randomizedLoadBalancer) selectServer(serviceName string, serviceProviderList_ serviceProviderList, _ uintptr, excludedServerList *markingList) (string, bool) {
	value, _ := self.serviceName2State.LoadOrStore(serviceName, &randomizedState{})
	state := value.(*randomizedState)
	var serviceProviderIndexes []int
	state.lock.Lock()

	if state.serviceProviderList.version < serviceProviderList_.version {
		for i := range serviceProviderList_.items {
			serviceProvider_ := &serviceProviderList_.items[i]

			for n := int(serviceProvider_.weight); n >= 1; n-- {
				serviceProviderIndexes = append(serviceProviderIndexes, i)
			}
		}

		state.serviceProviderList = serviceProviderList_
		state.serviceProviderIndexes = serviceProviderIndexes
	} else {
		serviceProviderList_ = state.serviceProviderList
		serviceProviderIndexes = state.serviceProviderIndexes
	}

	state.lock.Unlock()

	if len(serviceProviderList_.items) == 0 {
		return "", false
	}

	excludedServerList.unmarkItems()

	for {
		serviceProviderIndex := serviceProviderIndexes[rand.Intn(len(serviceProviderIndexes))]
		serviceProvider_ := &serviceProviderList_.items[serviceProviderIndex]

		if excludedServerList.markItem(serviceProvider_.serverAddress) {
			if excludedServerList.getNumberOfMarkedItems() == len(serviceProviderList_.items) {
				return "", false
			}

			continue
		}

		return serviceProvider_.serverAddress, true
	}
}

type roundRobinState struct {
	lock                     sync.Mutex
	serviceProviderList      serviceProviderList
	minWeight                int32
	initialSumOfWeights      int32
	nextServiceProviderIndex int
}

type roundRobinLoadBalancer struct {
	serviceName2State sync.Map
}

func (self *roundRobinLoadBalancer) selectServer(serviceName string, serviceProviderList_ serviceProviderList, _ uintptr, excludedServerList *markingList) (string, bool) {
	value, _ := self.serviceName2State.LoadOrStore(serviceName, &roundRobinState{})
	state := value.(*roundRobinState)
	state.lock.Lock()

	if state.serviceProviderList.version < serviceProviderList_.version {
		var minWeight int32
		var nextServiceProviderIndex int

		if numberOfServiceProviders := len(serviceProviderList_.items); numberOfServiceProviders == 0 {
			minWeight = 0
			nextServiceProviderIndex = 0
		} else {
			minWeight = math.MaxInt32

			for i := range serviceProviderList_.items {
				serviceProvider_ := &serviceProviderList_.items[i]

				if serviceProvider_.weight < minWeight {
					minWeight = serviceProvider_.weight
				}
			}

			nextServiceProviderIndex = rand.Intn(numberOfServiceProviders)
		}

		state.serviceProviderList = serviceProviderList_
		state.minWeight = minWeight
		state.initialSumOfWeights = 0
		state.nextServiceProviderIndex = nextServiceProviderIndex
	}

	if len(state.serviceProviderList.items) == 0 {
		state.lock.Unlock()
		return "", false
	}

	excludedServerList.unmarkItems()
	n := len(state.serviceProviderList.items)

	for {
		serviceProvider_ := &state.serviceProviderList.items[state.nextServiceProviderIndex]
		sumOfWeights := state.initialSumOfWeights + serviceProvider_.weight

		if sumOfWeights < 2*state.minWeight {
			state.nextServiceProviderIndex = (state.nextServiceProviderIndex + 1) % n
			state.initialSumOfWeights = sumOfWeights - state.minWeight
		} else {
			state.initialSumOfWeights -= state.minWeight
		}

		if excludedServerList.markItem(serviceProvider_.serverAddress) {
			if excludedServerList.getNumberOfMarkedItems() == len(serviceProviderList_.items) {
				state.lock.Unlock()
				return "", false
			}

			continue
		}

		state.lock.Unlock()
		return serviceProvider_.serverAddress, true
	}
}

type consistentHashingState struct {
	lock                          sync.Mutex
	serviceProviderList           serviceProviderList
	hashCodes                     []uint32
	hashCode2ServiceProviderIndex map[uint32]int
}

type consistentHashingLoadBalancer struct {
	serviceName2State sync.Map
}

func (self *consistentHashingLoadBalancer) selectServer(serviceName string, serviceProviderList_ serviceProviderList, lbArgument uintptr, excludedServerList *markingList) (string, bool) {
	hashCode := uint32(lbArgument)
	value, _ := self.serviceName2State.LoadOrStore(serviceName, &consistentHashingState{})
	state := value.(*consistentHashingState)
	var hashCodes []uint32
	var hashCode2ServiceProviderIndex map[uint32]int
	state.lock.Lock()

	if state.serviceProviderList.version < serviceProviderList_.version {
		for i := range serviceProviderList_.items {
			serviceProvider_ := &serviceProviderList_.items[i]
			hashCode2ServiceProviderIndex = map[uint32]int{}

			for n := 16 * int(serviceProvider_.weight); n >= 1; n-- {
				hash := fnv.New64a()
				hash.Write([]byte(strconv.Itoa(n) + "-" + serviceProvider_.serverAddress))
				hashCode := uint32(hash.Sum64())
				hashCodes = append(hashCodes, hashCode)
				hashCode2ServiceProviderIndex[hashCode] = i
			}

			sort.Slice(hashCodes, func(i, j int) bool {
				return hashCodes[i] < hashCodes[j]
			})
		}

		state.serviceProviderList = serviceProviderList_
		state.hashCodes = hashCodes
		state.hashCode2ServiceProviderIndex = hashCode2ServiceProviderIndex
	} else {
		serviceProviderList_ = state.serviceProviderList
		hashCodes = state.hashCodes
		hashCode2ServiceProviderIndex = state.hashCode2ServiceProviderIndex
	}

	state.lock.Unlock()

	if len(serviceProviderList_.items) == 0 {
		return "", false
	}

	excludedServerList.unmarkItems()
	n := len(hashCodes)

	i := sort.Search(n, func(i int) bool {
		return hashCodes[i] >= hashCode
	})

	if i == n {
		i = 0
	}

	for {
		serviceProviderIndex := hashCode2ServiceProviderIndex[hashCodes[i]]
		serviceProvider_ := &serviceProviderList_.items[serviceProviderIndex]

		if excludedServerList.markItem(serviceProvider_.serverAddress) {
			if excludedServerList.getNumberOfMarkedItems() == len(serviceProviderList_.items) {
				return "", false
			}

			i = (i + 1) % n
			continue
		}

		return serviceProvider_.serverAddress, true
	}
}
