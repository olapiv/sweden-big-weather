import kafkaNode from 'kafka-node';
const { KafkaClient, Consumer, Message, Offset, OffsetFetchRequest, ConsumerOptions } = kafkaNode;

const KAFKA_HOST = 'kafka:9092';

export const setUpClient = () => {
    const clientOptions = { kafkaHost: KAFKA_HOST, connectTimeout: 13000, requestTimeout: 13000, autoConnect: true }
    const client = new KafkaClient(clientOptions);

    client.on('connect', function (err) {
        if (err) {
            throw err
        }
        console.log("Connected client!");
    });

    client.on('error', (err) => {
        throw err
    });

    return client
}

export const setupConsumer = (client, topic, funcOnMessageReceive) => {

    const topics = [{ topic: topic, partitions: 1 }];
    const consumerOptions = { autoCommit: false };
    const consumer = new Consumer(client, topics, consumerOptions);

    consumer.on('message', (message) => {
        console.log("message.value: ", message.value);
        funcOnMessageReceive("[" + message.value + "]");
    })

    consumer.on('error', function (err) {
        throw err
    });

    const offset = new Offset(client);

    // If consumer get `offsetOutOfRange` event, fetch data from the smallest(oldest) offset            
    consumer.on(
        'offsetOutOfRange',
        (topic) => {
            offset.fetch([topic], function (err, offsets) {
                if (err) {
                    return console.error(err);
                }
                const min = Math.min.apply(null, offsets[topic.topic][topic.partition]);
                consumer.setOffset(topic.topic, topic.partition, min);
            });
        }
    );

}