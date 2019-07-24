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
	SelectServer(serviceName string, serviceProviderList_ serviceProviderList, lbArgument uintptr, excludedServerList *markingList) (serverAddress string, ok bool)
}

type serviceProviderList struct {
	items   []serviceProvider
	version int64
}

func (self *serviceProviderList) FindServer(serverID int32) (string, bool) {
	if n := len(self.items); n >= 1 {
		i := 0
		j := n - 1

		for i < j {
			k := (i + j) / 2

			if self.items[k].ServerID < serverID {
				i = k + 1
			} else {
				j = k
			}
		}

		if self.items[i].ServerID == serverID {
			return self.items[i].ServerAddress, true
		}
	}

	return "", false
}

type serviceProvider struct {
	ServerAddress string
	Weight        int32
	ServerID      int32
}

type randomizedState struct {
	Lock                   sync.Mutex
	ServiceProviderList    serviceProviderList
	ServiceProviderIndexes []int
}

type randomizedLoadBalancer struct {
	serviceName2State sync.Map
}

func (self *randomizedLoadBalancer) SelectServer(serviceName string, serviceProviderList_ serviceProviderList, _ uintptr, excludedServerList *markingList) (string, bool) {
	value, _ := self.serviceName2State.LoadOrStore(serviceName, &randomizedState{})
	state := value.(*randomizedState)
	var serviceProviderIndexes []int
	state.Lock.Lock()

	if state.ServiceProviderList.version < serviceProviderList_.version {
		for i := range serviceProviderList_.items {
			serviceProvider_ := &serviceProviderList_.items[i]

			for n := int(serviceProvider_.Weight); n >= 1; n-- {
				serviceProviderIndexes = append(serviceProviderIndexes, i)
			}
		}

		state.ServiceProviderList = serviceProviderList_
		state.ServiceProviderIndexes = serviceProviderIndexes
	} else {
		serviceProviderList_ = state.ServiceProviderList
		serviceProviderIndexes = state.ServiceProviderIndexes
	}

	state.Lock.Unlock()

	if len(serviceProviderList_.items) == 0 {
		return "", false
	}

	excludedServerList.UnmarkItems()

	for {
		serviceProviderIndex := serviceProviderIndexes[rand.Intn(len(serviceProviderIndexes))]
		serviceProvider_ := &serviceProviderList_.items[serviceProviderIndex]

		if excludedServerList.MarkItem(serviceProvider_.ServerAddress) {
			if excludedServerList.GetNumberOfMarkedItems() == len(serviceProviderList_.items) {
				return "", false
			}

			continue
		}

		return serviceProvider_.ServerAddress, true
	}
}

type roundRobinState struct {
	Lock                     sync.Mutex
	ServiceProviderList      serviceProviderList
	MinWeight                int32
	InitialSumOfWeights      int32
	NextServiceProviderIndex int
}

type roundRobinLoadBalancer struct {
	serviceName2State sync.Map
}

func (self *roundRobinLoadBalancer) SelectServer(serviceName string, serviceProviderList_ serviceProviderList, _ uintptr, excludedServerList *markingList) (string, bool) {
	value, _ := self.serviceName2State.LoadOrStore(serviceName, &roundRobinState{})
	state := value.(*roundRobinState)
	state.Lock.Lock()

	if state.ServiceProviderList.version < serviceProviderList_.version {
		var minWeight int32
		var nextServiceProviderIndex int

		if numberOfServiceProviders := len(serviceProviderList_.items); numberOfServiceProviders == 0 {
			minWeight = 0
			nextServiceProviderIndex = 0
		} else {
			minWeight = math.MaxInt32

			for i := range serviceProviderList_.items {
				serviceProvider_ := &serviceProviderList_.items[i]

				if serviceProvider_.Weight < minWeight {
					minWeight = serviceProvider_.Weight
				}
			}

			nextServiceProviderIndex = rand.Intn(numberOfServiceProviders)
		}

		state.ServiceProviderList = serviceProviderList_
		state.MinWeight = minWeight
		state.InitialSumOfWeights = 0
		state.NextServiceProviderIndex = nextServiceProviderIndex
	}

	if len(state.ServiceProviderList.items) == 0 {
		state.Lock.Unlock()
		return "", false
	}

	excludedServerList.UnmarkItems()
	n := len(state.ServiceProviderList.items)

	for {
		serviceProvider_ := &state.ServiceProviderList.items[state.NextServiceProviderIndex]
		sumOfWeights := state.InitialSumOfWeights + serviceProvider_.Weight

		if sumOfWeights < 2*state.MinWeight {
			state.NextServiceProviderIndex = (state.NextServiceProviderIndex + 1) % n
			state.InitialSumOfWeights = sumOfWeights - state.MinWeight
		} else {
			state.InitialSumOfWeights -= state.MinWeight
		}

		if excludedServerList.MarkItem(serviceProvider_.ServerAddress) {
			if excludedServerList.GetNumberOfMarkedItems() == len(serviceProviderList_.items) {
				state.Lock.Unlock()
				return "", false
			}

			continue
		}

		state.Lock.Unlock()
		return serviceProvider_.ServerAddress, true
	}
}

type consistentHashingState struct {
	Lock                          sync.Mutex
	ServiceProviderList           serviceProviderList
	HashCodes                     []uint32
	HashCode2ServiceProviderIndex map[uint32]int
}

type consistentHashingLoadBalancer struct {
	serviceName2State sync.Map
}

func (self *consistentHashingLoadBalancer) SelectServer(serviceName string, serviceProviderList_ serviceProviderList, lbArgument uintptr, excludedServerList *markingList) (string, bool) {
	hashCode := uint32(lbArgument)
	value, _ := self.serviceName2State.LoadOrStore(serviceName, &consistentHashingState{})
	state := value.(*consistentHashingState)
	var hashCodes []uint32
	var hashCode2ServiceProviderIndex map[uint32]int
	state.Lock.Lock()

	if state.ServiceProviderList.version < serviceProviderList_.version {
		for i := range serviceProviderList_.items {
			serviceProvider_ := &serviceProviderList_.items[i]
			hashCode2ServiceProviderIndex = map[uint32]int{}

			for n := 16 * int(serviceProvider_.Weight); n >= 1; n-- {
				hash := fnv.New64a()
				hash.Write([]byte(strconv.Itoa(n) + "-" + serviceProvider_.ServerAddress))
				hashCode := uint32(hash.Sum64())
				hashCodes = append(hashCodes, hashCode)
				hashCode2ServiceProviderIndex[hashCode] = i
			}

			sort.Slice(hashCodes, func(i, j int) bool {
				return hashCodes[i] < hashCodes[j]
			})
		}

		state.ServiceProviderList = serviceProviderList_
		state.HashCodes = hashCodes
		state.HashCode2ServiceProviderIndex = hashCode2ServiceProviderIndex
	} else {
		serviceProviderList_ = state.ServiceProviderList
		hashCodes = state.HashCodes
		hashCode2ServiceProviderIndex = state.HashCode2ServiceProviderIndex
	}

	state.Lock.Unlock()

	if len(serviceProviderList_.items) == 0 {
		return "", false
	}

	excludedServerList.UnmarkItems()
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

		if excludedServerList.MarkItem(serviceProvider_.ServerAddress) {
			if excludedServerList.GetNumberOfMarkedItems() == len(serviceProviderList_.items) {
				return "", false
			}

			i = (i + 1) % n
			continue
		}

		return serviceProvider_.ServerAddress, true
	}
}
