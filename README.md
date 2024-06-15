### Run an async function with multiple consumers from a stream 
``` python
# import consumer from package
from redis_stream_consumer_utils.consumers.consumer import consumer

# create a redis connection object
redis_conn = redis.Redis(host='localhost', port=6379, decode_responses=True)

# attach an async function to the consumer imported and run via asyncio 
asyncio.run(consumer.start_consumer(redis_conn, "<stream_name>", "<number_of_consumers>", "<async_function>"))
```