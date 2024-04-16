package adapter

type (
	Producer interface {
		Producer(topic, key string, value []byte) error
		Close()
	}
	Consumer interface {
		Close()
	}
)
