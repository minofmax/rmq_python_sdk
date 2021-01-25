import sys
import time
from threading import Thread

import pika
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection


class MqBase(object):
    def __init__(self, host='localhost', port=5672, exchange='test_exchange', exchange_type='direct', name='test',
                 pwd='test_1234', ack=True, persist=True):
        self.host = host
        self.port = port
        self.name = name
        self.pwd = pwd
        self.auth = pika.PlainCredentials(self.name, self.pwd)
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.ack = ack
        self.persist = persist
        self.channel = None
        self.connection = None

    def _open_channel(self):
        """
        开启信道
        :return:
        """
        self.connection: BlockingConnection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host,
                                                                                                port=self.port,
                                                                                                credentials=self.auth))
        self.channel: BlockingChannel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange, exchange_type=self.exchange_type,
                                      durable=self.persist)
        if self.ack:
            self.channel.confirm_delivery()

    def _close_channel(self):
        """
        关闭信道
        :return:
        """
        if self.channel and self.channel.is_open:
            self.channel.close()

        if self.connection and self.connection.is_open:
            self.connection.close()


class MqProducer(MqBase):
    """
    消息队列-生产者
    """

    def send(self, msg=' ', route='test_queue'):
        self._open_channel()

        properties = pika.BasicProperties(delivery_mode=2 if self.persist else 0)
        self.channel.confirm_delivery()
        self.channel.basic_publish(exchange=self.exchange,
                                   routing_key=route,
                                   body=msg,
                                   properties=properties)
        self._close_channel()


def test_func(msg):
    print('this is msg:{}'.format(msg))


class MqConsumer(MqBase):
    """
    消息队列-消费者
    """
    func = None

    def _declare_queue(self, queue_name='test_queue'):
        """
        声明队列
        :param queue_name:
        :return:
        """
        self.channel.queue_declare(queue=queue_name, durable=self.persist)
        self.channel.queue_bind(queue=queue_name, exchange=self.exchange, routing_key=queue_name)

    def _subscribe_queue(self, queue_name='test_queue'):
        """
        订阅队列
        :param queue_name:
        :return:
        """
        self._declare_queue(queue_name=queue_name)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(on_message_callback=self.handler,
                                   queue=queue_name,
                                   auto_ack=not self.ack)

    def start(self, queue_name='test_queue', func=test_func):
        """
        开始消费
        :return:
        """
        self.func = func
        self._open_channel()
        self._subscribe_queue(queue_name)  # 订阅消息队列
        self.channel.start_consuming()  # 开始消费, 会回调handler方法

    def end(self):
        """
        结束消费
        :return:
        """
        self.channel.stop_consuming()
        self._close_channel()

    def handler(self, channel, method_frame, header_frame, body):
        """
        默认回调函数，处理消息的真正handler
        :param channel:
        :param method_frame:
        :param header_frame:
        :param body:
        :return:
        """
        self.func(body)
        if self.ack:  # 如果需要手动确认消息已经被消费的话, 则执行下一步确认操作
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)


if __name__ == '__main__':
    try:
        def consumer(num):
            def test(msg):
                print('consumer num:{}, consume msg:{}, sleep:{}s'.format(num, msg, 5))
                time.sleep(5)

            mqc = MqConsumer()
            mqc.start('test_queue', func=test)
            mqc.end()


        def producer():
            for msg in range(18):
                print('producer send %s' % msg)
                mqp = MqProducer()
                mqp.send(msg=str(msg), route='test_queue')


        for i in range(3):
            Thread(target=consumer, args=(i + 1,)).start()  # 新起线程去创建消费者, 监听固定的信道

        Thread(target=producer).start()  # 生产者
    except KeyboardInterrupt as e:
        sys.exit(0)
