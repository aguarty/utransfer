package main

import (
	"time"

	"github.com/kelseyhightower/envconfig"
)

type config struct {
	SRC struct {
		KeepConnection bool   `default:"true"`
		Source         string `required:"true" default:"clusterID=test-cluster host=localhost port=4223 pubTopic=topic007sub subTopic=topic007pub"`
		//Source string `required:"true" default:"user=postgres password=postgrespass8 dbname=postgres host=localhost port=5432 sslmode=disable"`
	}
	DST struct {
		KeepConnection bool `default:"true"`
		//Source         string `required:"true" default:"clusterID=test-cluster host=localhost port=4223 pubTopic=topic007pub subTopic=topic007sub"`
		Source string `required:"true" default:"user=postgres password=postgrespass8 dbname=postgres host=localhost port=5432 sslmode=disable"`
	}
	Queries struct {
		Select  string `required:"true" default:"select * from public.get_json_data($1);"`
		Insert  string `required:"true" default:"select random()"`
		Commit  string `required:"true" default:"select from public.del_rows($1);"`
		Process string
	}
	Delay struct {
		Loop    time.Duration `default:"500ms"`
		IfError time.Duration `default:"5s"`
		IfEmpty time.Duration `default:"1s"`
	}
	Limit struct {
		Process int `default:"10"`
	}
	LogLevel string `envconfig:"LOG_LEVEL" default:"debug" desc:"Уровень логирования(debug,info,warn,error,fatal)"`
}

func (a *application) initConfig() error {
	var envConfig config
	if err := envconfig.Process("", &envConfig); err != nil {
		envconfig.Usage("", &envConfig)
		return err
	}
	a.cfg = envConfig
	return nil
}
