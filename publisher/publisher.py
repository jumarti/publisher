from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import CancelledError

import logging
import os
import random
import redis
import sys
import time
import threading
import pdb

logging.basicConfig(level=logging.DEBUG)
logger = logging

def as_thread(cls):
    return type(cls.__name__, (threading.Thread,), dict(cls.__dict__))

# @as_thread # enable this decorator so Publisher can be a thread
class Publisher:
    def __init__(self, max_workers=4):
        self._pubpool = ThreadPoolExecutor(max_workers=max_workers)
        self._in_transit = {}
        #the next super() signature is required for the as_thread hack to work
        super(Publisher, self).__init__()


    def _push_new(self, message):
        ti = time.time()
        queue_size = len(self._in_transit)
        if queue_size > 50:
            logger.error(f"pub queue is too high, wont publish ({queue_size})")
            return False

        if queue_size > 10:
            logger.warning(f"queue is high ({queue_size})")

        future = self._pubpool.submit(self.publish, message)
        future.add_done_callback(self._on_publish_done)
        self._in_transit[future] = (message, ti)
        logger.debug(f"pushed a new message={message} queue_size={queue_size}")
        return True

    def _on_publish_done(self, future):
        message, ti = self._in_transit.pop(future)
        result = None
        try:
            result = future.result()
            error = None
        except Exception as _error:
            error = _error
        duration = time.time() - ti
        logger.debug(
            f"publish done message={message} result={result} error={error} "
            f"duration={duration:0.3f}"
            )
        self.on_done(message, result, error, duration )

    def publish(self, message):
        raise NotImplementedError

    def on_done(self, message, result, error, duration):
        raise NotImplementedError

    def consume_loop(self):
        raise NotImplementedError

    def run(self):
        return self.consume_loop()


class AMQPPublisher(Publisher):
    def consume_loop(self):
        with kombu.Connection("amqp://guest:guest@localhost:5672//") as conn:
            queue = conn.SimpleQueue('foo_queue')
            while True:
                message = queue.get(block=True)
                if self._push_new(message) == True:
                    continue

                congested = True
                while congested:
                    print("under congestion, holding consumption")
                    time.sleep(5)
                    congested = not self._push_new(message)


    def publish(self, message):
        time.sleep(1) # request.post()
        return "done"

    def on_done(self, message, result, error, duration):
        if error is None:
            message.ack() #AMQP ack

class RedisSubPublisher(Publisher):
    def consume_loop(self):
        rconn = redis.StrictRedis()
        subs = rconn.pubsub()
        subs.subscribe('test_channel')
        while True:
            message = subs.get_message(ignore_subscribe_messages=True,timeout=5)
            if message:
                self._push_new(message.get('data'))

    def publish(self, message):
        dt = random.randint(1000,5000) / 1000
        time.sleep(dt), #simulating a request.post send
        if dt > 4.5:
            raise Exception("simulating an error")

        return "request 200" #simualting a response

    def on_done(self, message, result, error, duration):
        if error is None:
            logger.info(
                f"Publish OK: (result={result}) in ({duration:0.2f} secs) "
                f"for (message={message})"
                )
        else:
            logger.error(
                f"Publisherublish ERROR: (error={error}) in ({duration:0.2f} secs) "
                f"for (message={message})"
                )

        return


def main():
    consume_publish = RedisSubPublisher(max_workers=50)
    consume_publish2 = RedisSubPublisher(max_workers=50)
    consume_publish.run()

    # if using # @as_thread on Publisher class:
    # consume_publish.start()
    # consume_publish.join()

    # test publish from bash:
    # $ watch -n 0.5 "redis-cli publish test_channel test-message"

if __name__ == '__main__':
    main()