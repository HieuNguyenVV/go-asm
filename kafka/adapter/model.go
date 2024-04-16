package adapter

type (
	ConsumerConfig struct {
		Config Config `json:"config" yaml:"config"`
		Topics string `yaml:"topics" json:"topics"`
		Group  string `json:"group" yaml:"group"`
	}
	ProducerConfig struct {
		Config Config `json:"config" yaml:"config"`
		Async  bool   `yaml:"async" json:"async"`
	}
	Config struct {
		Address string `yaml:"address" json:"address"`
	}

	Event struct {
		Msg      string `json:"msg"`
		CreateAt int64  `json:"create_at"`
	}
)
