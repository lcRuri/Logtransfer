package main

import (
	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
	"logtransfer/es"
	"logtransfer/kafka"
	"logtransfer/model"
)

//log transfer
//从kafka消费日志数据，写入es

func main() {
	var cfg = new(model.Config)
	err := ini.MapTo(cfg, "./config/logtransfer.ini")
	if err != nil {
		logrus.Errorf("load config failed,err:%v", err)
		panic(err)
	}
	logrus.Info("load config success")

	//连接es
	err = es.Init(cfg.ESConf.Address, cfg.ESConf.Index, cfg.ESConf.MaxSize, cfg.ESConf.GoNum)
	if err != nil {
		logrus.Errorf("Init es failed,err:%v", err)
		panic(err)
	}

	logrus.Info("Init es success")

	//连接kafka
	err = kafka.Init([]string{cfg.KafkaConf.Address}, cfg.KafkaConf.Topic)
	if err != nil {
		logrus.Errorf("connect to kafka failed,err:%v", err)
		panic(err)
	}

	logrus.Info("Init kafka success")

	select {}

}
