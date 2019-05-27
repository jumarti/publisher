# Concurrent Publisher

Uses `concurrent.futures.ThreadPoolExecutor` to concurrently apply blocking calls over messages consumed in a loop.

For example: Consume an AMPQ queue and republish the messages via http using `request` 

### Usage
#### 1.Define a class inheriting from Publisher
Implement the following methods:

-  `consume_loop(self)` : 
Implement an infinite loop waiting for your input source. 
On new messages, call `self._push_new(message)`
If this call returns `True`, the message was succesfully accepted and it would be handled by the publishing workers.
If it returns False, it means that there are too many messages in transit thus new messages can not be pushed


- `publish(self, message)`: Implement the blocking publishing. 
For example, `request.post(..., data=message)`
Is not necessary to handle exceptions here, let them raise for the `on_done` call to handle
Return a `response` that you want to use on the `on_done` call


- `on_done(self, message, result, error, duration)`:
Implement the result handling.
If error is `None`, the publishing was successful, `result` holds the returned value from `publish`. 
`error` may hold any exception raised during publishing.
Duration is the time since from `_push_message` to `on_done` being called.

#### 2. On your main loop/thread 
Create an instance of your class and execute the blocking call `.run()`
The class accepts the following parameters:
- `max_workers`: Max number of publishing thread (see `ThreadPoolExecutor`)
- `queue_size_stop` : Stop receiving new messages if max. messages in transit reaches this value.
- `queue_size_warning` : Log warning message if max. messages in transit exceeds this value.

#### Example

```python
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



if __name__ == '__main__':
    publisher = RedisSubPublisher(max_workers=10)
    publisher.run()

```