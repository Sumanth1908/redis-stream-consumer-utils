import asyncio
import datetime

import redis_stream_consumer_utils.utils.logger as custom_logger

logger = custom_logger.setup_logger(__name__)


async def read(redis_connection, stream_name, consumer_group_name, consumer_name, target_function, check_backlog=False):
    """
    Reads a single message from the Redis Stream asynchronously.
    :param check_backlog: This flag is used to check backlog items in a stream.
    :param redis_connection: The Redis connection object.
    :param stream_name: The name of the Redis Stream.
    :param consumer_group_name: The name of the consumer group.
    :param consumer_name: The name of this specific consumer.
    :param target_function: The target function to execute.
    :return: A list containing the processed message details (if any).
    """

    try:
        check_latest = True
        max_message_read_count = 1
        if check_backlog:
            last_id = "0-0"
            logger.info("Checking backlog for pending items")
            result = redis_connection.xreadgroup(streams={stream_name: last_id},
                                                 groupname=consumer_group_name,
                                                 consumername=consumer_name,
                                                 count=max_message_read_count)
            logger.info(f"{consumer_name} received {result}")
            for record in result:
                _, message = record
                if message:
                    message_id, data = message[0]
                    logger.info(f"Executing target function with parameters : {data}")
                    await target_function(data)
                    logger.info(f"Consumer {consumer_name} acknowledging message : {message_id}")
                    ack_status = redis_connection.xack(stream_name, consumer_group_name, message_id)
                    logger.info(f"Consumer {consumer_name} acknowledging message : {message_id} status : {ack_status}")
                else:
                    check_latest = True

        if check_latest:
            last_id = ">"
            logger.info("Checking with latest id")
            result = redis_connection.xreadgroup(streams={stream_name: last_id},
                                                 groupname=consumer_group_name,
                                                 consumername=consumer_name,
                                                 count=max_message_read_count)
            logger.info(f"{consumer_name} received {result}")
            for record in result:
                _, message = record
                if message:
                    message_id, data = message[0]
                    logger.info(f"Executing target function with parameters : {data}")
                    await target_function(data)
                    logger.info(f"Consumer {consumer_name} acknowledging message : {message_id}")
                    ack_status = redis_connection.xack(stream_name, consumer_group_name, message_id)
                    logger.info(f"Consumer {consumer_name} acknowledging message : {message_id} status : {ack_status}")

        return True
    except Exception as e:
        logger.error(f"Exception : {e} occurred")
        return False


async def start_consumer(redis_connection, redis_stream_name, consumers, target_function, sleep_time=5):
    """
    Start an asynchronous consumers and execute a target function for items received.
    :param redis_connection: A redis connection object.
    :param redis_stream_name: Name of the stream to start consumption.
    :param consumers: Number of consumers required.
    :param target_function: Target async function which needs to be executed.
    :param sleep_time: Sleep time in seconds.
    :return: None
    """
    consumer_group = f"{redis_stream_name}_consumers"
    logger.info(f"Created consumer group with name {consumer_group}")
    try:
        await redis_connection.xgroup_create(redis_stream_name, consumer_group)
    except Exception as e:
        logger.info(f"Exception : {e} creating consumer group, proceeding")
        pass

    # creating consumers
    consumers = [f'consumer_{_}' for _ in range(1, consumers + 1)]
    logger.info(f"Created consumers : {consumers}")

    while True:
        # creating coroutine/task list
        tasks = [read(redis_connection, redis_stream_name, consumer_group, consumer, target_function)
                 for consumer in consumers]
        logger.info(f"Created coroutines of size : {len(tasks)}")
        logger.info(f"Started execution at {datetime.datetime.now()}")
        # Run consumers concurrently
        results = await asyncio.gather(*tasks)
        logger.info(f"Task results {results}")
        await asyncio.sleep(sleep_time)
