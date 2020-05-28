package main

import (
	"fmt"
	"github.com/aliyun/aliyun-log-go-sdk"
	"github.com/aliyun/aliyun-log-go-sdk/consumer"
	"github.com/go-kit/kit/log/level"
	"os"
	"os/signal"
	"sync"
	"time"
)

// README :
//   	该demo用来重置消费位点，当消费组已经存在，重新启动消费组不想去消费存量数据，从当前
// 时间点进行消费，请使用该demo。 请在 getCursor 函数里面使用自己的ak, project, logstore。
// 逻辑:
//      当启动该消费组，拉取数据后，在process消费函数里面进行判断, 如果shard 不在全局变量
// shardMap 里面，就重置消费位点为当前时间的cursor, 从当前时间进行消费，不在消费存量数据。


var shardMap = map[int]string{}
var lock sync.Mutex
func main() {
	option := consumerLibrary.LogHubConfig{
		Endpoint:          "",
		AccessKeyID:       "",
		AccessKeySecret:   "",
		Project:           "",
		Logstore:          "",
		ConsumerGroupName: "",
		ConsumerName:      "",
		// This options is used for initialization, will be ignored once consumer group is created and each shard has been started to be consumed.
		// Could be "begin", "end", "specific time format in time stamp", it's log receiving time.
		CursorPosition: consumerLibrary.BEGIN_CURSOR,
	}
	consumerWorker := consumerLibrary.InitConsumerWorker(option, process)
	ch := make(chan os.Signal)
	signal.Notify(ch)
	consumerWorker.Start()
	if _, ok := <-ch; ok {
		level.Info(consumerWorker.Logger).Log("msg", "get stop signal, start to stop consumer worker", "consumer worker name", option.ConsumerName)
		consumerWorker.StopAndWait()
	}
}

// Fill in your consumption logic here, and be careful not to change the parameters of the function and the return value,
// otherwise you will report errors.
func process(shardId int, logGroupList *sls.LogGroupList) string {
	// 这里填入自己的消费逻辑
	return consumptionFromCurrentTime(shardId)
}

func getCursor(shardId int) string {
	client := sls.Client{
		Endpoint:"your  endponrt",
		AccessKeySecret:"your AccessKeySecret",
		AccessKeyID: "your AccessKeyID",
	}
	from := fmt.Sprintf("%d", time.Now().Unix())
	cursor, err := client.GetCursor("your project","your logstore", shardId, from)
	if err != nil {
		fmt.Println(err)
	}
	return cursor
}

func consumptionFromCurrentTime(shardId int) string{
	lock.Lock()
	defer  lock.Unlock()
	if _, ok := shardMap[shardId]; ok {
		return ""
	}else{
		shardMap[shardId] = ""
		return getCursor(shardId)
	}
}