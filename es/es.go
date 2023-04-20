package es

import (
	"context"
	"github.com/olivere/elastic/v7"
	"github.com/sirupsen/logrus"
)

type ESClient struct {
	client      *elastic.Client
	index       string           //数据库
	logDataChan chan interface{} //接受日志的channel

}

var (
	esClient = &ESClient{}
)

// 将日志数据写入es
func Init(addr, index string, maxSize, goroutineNum int) (err error) {
	client, err := elastic.NewClient(elastic.SetURL("http://"+addr), elastic.SetSniff(false))
	if err != nil {
		logrus.Errorf("init es failed,err:%v", err)
		panic(err)
	}

	esClient.client = client
	esClient.index = index
	esClient.logDataChan = make(chan interface{}, maxSize)

	logrus.Info("connect to es success")

	for i := 0; i < goroutineNum; i++ {
		go sendToES()
	}

	return
}

func sendToES() {
	for m1 := range esClient.logDataChan {

		put, err := esClient.client.Index().Index(esClient.index).BodyJson(m1).Do(context.Background())
		if err != nil {
			logrus.Errorf("esClient failed,err:%v", err)
			return
		}
		logrus.Info("Index user %s to index %s,type %s\n", put.Id, put.Index, put.Type)
	}
}
func PutLogData(msg interface{}) {
	esClient.logDataChan <- msg
}
