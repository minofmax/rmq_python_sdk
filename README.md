# rmq_python_sdk
## 一、简介
基于 python3 实现的rmq的sdk，提供了producer和consumer的封装接口，可以直接调用。支持传参配置exchange、queue_name、exchange_type、ack等参数。
## 二、使用
安装依赖，这里主要是pika
```shell
pip install pika
```
使用示例：
```python3
try:
    def consumer(num):
    """
    定义消费者, handler实际执行的是test方法
    """
        def test(msg):
            print('consumer num:{}, consume msg:{}, sleep:{}s'.format(num, msg, 5))
            time.sleep(5)

            mqc = MqConsumer()
            mqc.start('test_queue', func=test) # 配置topic名称, 对应实际消费消息的方法
            mqc.end()


    def producer():
    """
    定义生产者
    """
        for msg in range(18):
            print('producer send %s' % msg)
            mqp = MqProducer()
            mqp.send(msg=str(msg), route='test_queue') # 产生消息, 塞入队列


    for i in range(3):
        Thread(target=consumer, args=(i + 1,)).start()  # 新起线程去创建消费者, 监听固定的信道

    Thread(target=producer).start()  # 生产者
except KeyboardInterrupt as e:
    sys.exit(0)
```
