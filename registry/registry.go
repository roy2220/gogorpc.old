package registry

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"runtime"
	"strconv"
	"sync"

	"github.com/hashicorp/consul/api"

	"github.com/let-z-go/gogorpc/server"
	"github.com/let-z-go/toolkit/uuid"
)

type Registry struct {
	options           *Options
	consulClient      *api.Client
	servicePrototypes map[*server.Options]map[string]*Service
	services          sync.Map
}

func (self *Registry) Init(options *Options, consulClient *api.Client) *Registry {
	self.options = options.Normalize()
	self.consulClient = consulClient
	return self
}

func (self *Registry) RegisterServices(servicePrototypes ...*Service) func(*server.Options) {
	return func(serverOptions *server.Options) {
		if len(servicePrototypes) == 0 {
			return
		}

		servicePrototypes2, ok := self.servicePrototypes[serverOptions]

		if !ok {
			serverOptions.Hooks = append(serverOptions.Hooks, &server.Hook{
				BeforeRun: func(ctx context.Context, serverURL *url.URL) error {
					servicePrototypes := self.servicePrototypes[serverOptions]
					return self.registerServices(ctx, serverURL, servicePrototypes)
				},

				AfterRun: func(serverURL *url.URL) {
					servicePrototypes := self.servicePrototypes[serverOptions]
					self.deregisterServices(serverURL, servicePrototypes)
				},
			})

			if self.servicePrototypes == nil {
				self.servicePrototypes = map[*server.Options]map[string]*Service{}
			}

			servicePrototypes2 = map[string]*Service{}
			self.servicePrototypes[serverOptions] = servicePrototypes2
		}

		for _, servicePrototype := range servicePrototypes {
			servicePrototypes2[servicePrototype.Name] = servicePrototype
		}
	}
}

func (self *Registry) registerServices(ctx context.Context, serverURL *url.URL, servicePrototypes map[string]*Service) error {
	port, err := net.DefaultResolver.LookupPort(ctx, "tcp", serverURL.Port())

	if err != nil {
		return err
	}

	services := make([]*Service, len(servicePrototypes))
	i := 0

	for _, servicePrototype := range servicePrototypes {
		serviceURL := *serverURL
		serviceURL.Fragment = servicePrototype.Name
		rawServiceURL := serviceURL.String()
		service := servicePrototype.clone(self.options.BasicServiceMeta, self.options.BasicServiceTags)

		if _, ok := self.services.LoadOrStore(rawServiceURL, service); ok {
			return &DuplicateServiceRegistrationError{fmt.Sprintf("serviceURL=%#v", rawServiceURL)}
		}

		service.Meta["_URL"] = rawServiceURL
		service.Meta["_Weight"] = strconv.Itoa(service.Weight)
		service.Meta["_GoVersion"] = runtime.Version()

		if err := self.consulClient.Agent().ServiceRegister(&api.AgentServiceRegistration{
			ID:      service.id.String(),
			Name:    service.Name,
			Tags:    service.Tags,
			Port:    port,
			Address: serverURL.Hostname(),
			Meta:    service.Meta,

			Check: &api.AgentServiceCheck{
				TCP:                            serverURL.Host,
				Timeout:                        "10s",
				Interval:                       "10s",
				DeregisterCriticalServiceAfter: "60s",
			},
		}); err != nil {
			self.options.Logger.Error().Err(err).
				Str("server_url", serverURL.String()).
				Str("service_name", service.Name).
				Str("service_id", service.id.String()).
				Msg("registry_service_register_failed")

			self.services.Delete(rawServiceURL)

			for i--; i >= 0; i-- {
				service = services[i]
				self.services.Delete(service.Meta["_URL"])

				if err := self.consulClient.Agent().ServiceDeregister(service.id.String()); err != nil {
					self.options.Logger.Error().Err(err).
						Str("server_url", serverURL.String()).
						Str("service_name", service.Name).
						Str("service_id", service.id.String()).
						Msg("registry_service_deregister_failed")
					continue
				}

				self.options.Logger.Info().
					Str("server_url", serverURL.String()).
					Str("service_name", service.Name).
					Str("service_id", service.id.String()).
					Msg("registry_service_deregistered")
			}

			return err
		}

		self.options.Logger.Info().Err(err).
			Str("server_url", serverURL.String()).
			Str("service_name", service.Name).
			Str("service_id", service.id.String()).
			Msg("registry_service_registered")

		services[i] = service
		i++
	}

	return nil
}

func (self *Registry) deregisterServices(serverURL *url.URL, servicePrototypes map[string]*Service) {
	for _, servicePrototype := range servicePrototypes {
		serviceURL := *serverURL
		serviceURL.Fragment = servicePrototype.Name
		rawServiceURL := serviceURL.String()
		value, _ := self.services.Load(rawServiceURL)
		self.services.Delete(rawServiceURL)
		service := value.(*Service)

		if err := self.consulClient.Agent().ServiceDeregister(service.id.String()); err != nil {
			self.options.Logger.Error().Err(err).
				Str("server_url", serverURL.String()).
				Str("service_name", service.Name).
				Str("service_id", service.id.String()).
				Msg("registry_service_deregister_failed")
			continue
		}

		self.options.Logger.Info().
			Str("server_url", serverURL.String()).
			Str("service_name", service.Name).
			Str("service_id", service.id.String()).
			Msg("registry_service_deregistered")
	}
}

type Service struct {
	Name   string
	Meta   map[string]string
	Tags   []string
	Weight int

	id uuid.UUID
}

func (self *Service) clone(basicMeta map[string]string, basicTags []string) *Service {
	clone := Service{
		id: uuid.GenerateUUID4Fast(),

		Name:   self.Name,
		Weight: self.Weight,
	}

	clone.Meta = make(map[string]string, len(basicMeta)+len(self.Meta))

	for k, v := range basicMeta {
		clone.Meta[k] = v
	}

	for k, v := range self.Meta {
		clone.Meta[k] = v
	}

	clone.Tags = make([]string, len(basicTags)+len(self.Tags))
	i := 0

	for _, tag := range basicTags {
		clone.Tags[i] = tag
		i++
	}

	for _, tag := range self.Tags {
		clone.Tags[i] = tag
		i++
	}

	return &clone
}

type DuplicateServiceRegistrationError struct {
	context string
}

func (self DuplicateServiceRegistrationError) Error() string {
	message := "gogorpc/registry: duplicate service registration"

	if self.context != "" {
		message += ": " + self.context
	}

	return message
}
