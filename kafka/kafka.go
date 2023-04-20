package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"logtransfer/es"
	"sync"
)

// 初始化kafka连接
// 从kafka中取出日志数据
func Init(address []string, topic string) (err error) {
	consumer, err := sarama.NewConsumer(address, nil)
	if err != nil {
		fmt.Printf("fail to start consumer,err:%v", err)
		return
	}
	partitionsList, err := consumer.Partitions(topic)
	if err != nil {
		fmt.Printf("fail to get list of partition,err:%v", err)
		return
	}
	fmt.Println(partitionsList)
	var wg sync.WaitGroup
	for partition := range partitionsList { // 遍历所有的分区
		// 针对每个分区创建一个对应的分区消费者
		var pc sarama.PartitionConsumer
		pc, err = consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("failed to start consumer for partition %d,err:%v\n", partition, err)
			return
		}
		//defer pc.AsyncClose()
		wg.Add(1)
		// 异步从每个分区消费信息
		// 将消费发送给es chan
		go func(sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				fmt.Println(msg.Topic, string(msg.Value))
				var m1 map[string]string
				err = json.Unmarshal(msg.Value, &m1)
				if err != nil {
					logrus.Errorf("unmarshal log failed,err:%v", err)
					continue
				}
				es.PutLogData(m1)
			}
		}(pc)

	}
	return
}
