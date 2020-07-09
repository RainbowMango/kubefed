/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// TODO: consider moving it to a more generic package.
package util

import (
	"container/heap"
	"time"
)

const (
	// TODO: Investigate what capacity is right.
	delayingDelivererUpdateChanCapacity = 1000
)

// DelayingDelivererItem is structure delivered by DelayingDeliverer to the
// target channel.
type DelayingDelivererItem struct {
	// Key under which the value was added to deliverer.
	Key string
	// Value of the item.
	Value interface{}
	// When the item should be delivered.
	DeliveryTime time.Time
}

type delivererHeap struct {
	keyPosition map[string]int
	data        []*DelayingDelivererItem
}

// Functions required by container.Heap.

func (dh *delivererHeap) Len() int { return len(dh.data) }
func (dh *delivererHeap) Less(i, j int) bool {
	return dh.data[i].DeliveryTime.Before(dh.data[j].DeliveryTime)
}
func (dh *delivererHeap) Swap(i, j int) {
	dh.keyPosition[dh.data[i].Key] = j
	dh.keyPosition[dh.data[j].Key] = i
	dh.data[i], dh.data[j] = dh.data[j], dh.data[i]
}

func (dh *delivererHeap) Push(x interface{}) {
	item := x.(*DelayingDelivererItem)
	dh.data = append(dh.data, item)
	dh.keyPosition[item.Key] = len(dh.data) - 1
}

func (dh *delivererHeap) Pop() interface{} {
	n := len(dh.data)
	item := dh.data[n-1]
	dh.data = dh.data[:n-1]
	delete(dh.keyPosition, item.Key)
	return item
}

// A structure that pushes the items to the target channel at a given time.
type DelayingDeliverer struct {
	// Channel to deliver the data when their time comes.
	targetChannel chan *DelayingDelivererItem // 数据最终出口
	// Store for data
	heap *delivererHeap // 使用小头堆来管理待发送事件，按待发送时间排序
	// Channel to feed the main goroutine with updates.
	updateChannel chan *DelayingDelivererItem // 数据更新管道，有点类似于缓冲区，所有数据进来都经过该缓冲区，由该缓冲区再进入堆。（使用该管道的好处：堆中插入、取出数据可以不用锁，可以在同一个select语句中完成。）
	// To stop the main goroutine.
	stopChannel chan struct{}
}

func NewDelayingDeliverer() *DelayingDeliverer {
	return NewDelayingDelivererWithChannel(make(chan *DelayingDelivererItem, 100))
}

func NewDelayingDelivererWithChannel(targetChannel chan *DelayingDelivererItem) *DelayingDeliverer {
	return &DelayingDeliverer{
		targetChannel: targetChannel,
		heap: &delivererHeap{
			keyPosition: make(map[string]int),
			data:        make([]*DelayingDelivererItem, 0),
		},
		updateChannel: make(chan *DelayingDelivererItem, delayingDelivererUpdateChanCapacity),
		stopChannel:   make(chan struct{}),
	}
}

// Deliver all items due before or equal to timestamp.
func (d *DelayingDeliverer) deliver(timestamp time.Time) { // 每次把到期的数据全部发送后退出
	for d.heap.Len() > 0 {
		if timestamp.Before(d.heap.data[0].DeliveryTime) { // 时间还未到，不发送
			return
		}
		item := heap.Pop(d.heap).(*DelayingDelivererItem) // 时间已到，从堆中Pop一个发送到指定管道
		d.targetChannel <- item
	}
}

func (d *DelayingDeliverer) run() { // 检查堆顶数据发送时间，时间到了就把堆顶元素发送到targetChannel管道
	for {
		now := time.Now()
		d.deliver(now) // 发送堆顶元素到targetChannel管道

		nextWakeUp := now.Add(time.Hour)
		if d.heap.Len() > 0 {
			nextWakeUp = d.heap.data[0].DeliveryTime
		}
		sleepTime := nextWakeUp.Sub(now)

		select {
		case <-time.After(sleepTime): // 堆顶元素还未到发送时间，一直睡到发送时间
			break // just wake up and process the data
		case item := <-d.updateChannel: // 新的数据到来（该数据可以已在堆中，也可能是新的）
			if position, found := d.heap.keyPosition[item.Key]; found { // 如果数据还未发送，且此次更新发送时间更超时，则修改发送时间
				if item.DeliveryTime.Before(d.heap.data[position].DeliveryTime) {
					d.heap.data[position] = item
					heap.Fix(d.heap, position)
				}
				// Ignore if later.
			} else { // 如果数据已经发送，则直接推入
				heap.Push(d.heap, item)
			}
		case <-d.stopChannel:
			return
		}
	}
}

// Starts the DelayingDeliverer.
func (d *DelayingDeliverer) Start() {
	go d.run()
}

// Stops the DelayingDeliverer. Undelivered items are discarded.
func (d *DelayingDeliverer) Stop() {
	close(d.stopChannel)
}

// Delivers value at the given time.
// 每次发送数据到updateChannel。
func (d *DelayingDeliverer) DeliverAt(key string, value interface{}, deliveryTime time.Time) {
	d.updateChannel <- &DelayingDelivererItem{
		Key:          key,
		Value:        value,
		DeliveryTime: deliveryTime,
	}
}

// Delivers value after the given delay.
func (d *DelayingDeliverer) DeliverAfter(key string, value interface{}, delay time.Duration) {
	d.DeliverAt(key, value, time.Now().Add(delay))
}

// Gets target channel of the deliverer.
// 消费者获取管道
func (d *DelayingDeliverer) GetTargetChannel() chan *DelayingDelivererItem {
	return d.targetChannel
}

// Starts Delaying deliverer with a handler listening on the target channel.
// 消费者只提供处理函数，不关心管道，这是最常用的方式。
func (d *DelayingDeliverer) StartWithHandler(handler func(*DelayingDelivererItem)) {
	go func() {
		for {
			select {
			case item := <-d.targetChannel:
				handler(item)
			case <-d.stopChannel:
				return
			}
		}
	}()
	d.Start()
}
