// Original here: https://gist.github.com/drochgenius/485cdb9e022618276be241a9a7247e5e#file-consumer-ts

import kafkaNode from 'kafka-node';
const {  KafkaClient, Consumer, Message, Offset, OffsetFetchRequest, ConsumerOptions } = kafkaNode;
import sampleJSON from '../static/data.json';

const kafkaHost = 'localhost:9092';

export function kafkaFakeSubscribe(topic, handleMessageFunc) {
    setInterval(
        () => {
            const dataString = JSON.stringify(sampleJSON);
            // console.log("dataString: ", dataString);
            handleMessageFunc(dataString)
        },
        2000
    )
}

export function kafkaSubscribe(topic, handleMessageFunc) {

    // TODO: Think about having one continuous client, as a function parameter

    // New KafkaClient connects directly to Kafka brokers.
    const client = new KafkaClient({ kafkaHost });

    const topics = [{ topic: topic, partition: 0 }];
    const options = { autoCommit: false, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024 * 1024 };

    const consumer = new Consumer(client, topics, options);

    consumer.on('error', function(err) {
        console.log('error', err);
    });

    client.refreshMetadata(
        [topic],
        (err) => {
            const offset = new Offset(client);

            if (err) {
                // throw err;
                console.log("Error in client.refreshMetadata: ", err);
                return
            }

            consumer.on('message', function(message) {
                handleMessageFunc(message.value);
            });

            /*
             * If consumer get `offsetOutOfRange` event, fetch data from the smallest(oldest) offset
             */
            consumer.on(
                'offsetOutOfRange',
                (topic) => {
                    offset.fetch([topic], function(err, offsets) {
                        if (err) {
                            return console.error(err);
                        }
                        const min = Math.min.apply(null, offsets[topic.topic][topic.partition]);
                        consumer.setOffset(topic.topic, topic.partition, min);
                    });
                }
            );
        }
    );

    // TODO: Perhaps return Consumer, so it can be cancelled when the browser closes
}