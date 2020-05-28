package main

import (
	"fmt"
	"github.com/aliyun/aliyun-log-go-sdk"
	"time"
)


// README：
//     只需在配置项中填入需要更新的消费组就可重置检查点，和控制台上重置消费点效果一样。
//     如需重置多个，配置多个即可，如下面示例。

type Config struct{
	Endpoint string
	AccessKeyID string
	AccessKeySecret string
	Project string
	Logstore string
	ConsumerGroupName string
}


func main(){
	configList := []Config{}
	// 配置第一个需要更新检查点的消费组
	config1 := Config{
		Endpoint:"cn-hangzhou.log.aliyuncs.com",
		AccessKeySecret:"****",
		AccessKeyID:"****",
		Project:"project",
		Logstore:"logstore",
		ConsumerGroupName:"name",

	}
	configList = append(configList, config1)

	// 配置第二个需要更新检查点的消费组，下面类推。
	config2 := Config{
		Endpoint:"cn-hangzhou.log.aliyuncs.com",
		AccessKeySecret:"****",
		AccessKeyID:"****",
		Project:"project",
		Logstore:"logstore",
		ConsumerGroupName:"name",

	}
	configList = append(configList, config2)


	for _,config := range configList{
		UpdateConsumerGroupCheckPoint(config)
	}


}


func updateCheckpoint(config Config,client sls.Client,shardId int) error  {
	from := fmt.Sprintf("%d", time.Now().Unix())
	cursor, err := client.GetCursor(config.Project,config.Logstore, shardId, from)
	if err != nil {
		fmt.Println(err)
	}
	return client.UpdateCheckpoint(config.Project,config.Logstore,config.ConsumerGroupName,"",shardId, cursor,true)

}

func UpdateConsumerGroupCheckPoint(config Config) {
	client := sls.Client{Endpoint:config.Endpoint,AccessKeyID:config.AccessKeyID,AccessKeySecret:config.AccessKeySecret}
	shards, err := client.ListShards(config.Project,config.Logstore)
	if err != nil {
		fmt.Println(err)
	}else {
		for _,v := range shards {
			err = updateCheckpoint(config, client, v.ShardID)
			if err != nil{
				fmt.Println(err)
			}
		}
	}
}