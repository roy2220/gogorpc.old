package channel

import (
	"context"
	"errors"
	"time"
)

func MakeSimpleServerAddressProvider(
	serverAddresses []string,
	maxNumberOfConnectRetries int,
	connectRetryInterval time.Duration,
) ServerAddressProvider {
	i := 0

	return func(ctx context.Context, connectRetryCount int) (string, error) {
		if connectRetryCount > maxNumberOfConnectRetries {
			return "", errors.New("pbrpc/channel: too many connect retries")
		}

		if connectRetryCount >= 1 {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(connectRetryInterval):
			}
		}

		serverAddress := serverAddresses[i%len(serverAddresses)]
		i++
		return serverAddress, nil
	}
}
