// Original here: https://gist.github.com/drochgenius/485cdb9e022618276be241a9a7247e5e#file-consumer-ts

import kafkaNode from 'kafka-node';
const { KafkaClient, Consumer, Message, Offset, OffsetFetchRequest, ConsumerOptions } = kafkaNode;
import sampleJSON from '../static/sampleData.json';
import { filledData, getRandomInRange } from './fakeDataGenerator.js';

const KAFKA_HOST = 'kafka:9092';

export function kafkaFakeSubscribe(topic, handleMessageFunc) {
    setInterval(
        () => {
            const newTemperature = filledData.map(element => {
                const minimalChange = getRandomInRange(-8, 8, 3)
                element["temperature_celsius"] += minimalChange
                return element
            })

            const dataString = JSON.stringify(newTemperature);
            // const dataString = JSON.stringify(sampleJSON);

            handleMessageFunc(dataString)
        },
        2000
    )
}

export function kafkaSubscribe(topic, handleMessageFunc) {

    // TODO: Think about having one continuous client, as a function parameter

    // New KafkaClient connects directly to Kafka brokers.
    const client = new KafkaClient(KAFKA_HOST, 3000, 3000, true );

    const topics = [{ topic: topic, partitions: 1 }];
    const options = { autoCommit: false, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024 * 1024 };

    const consumer = new Consumer(client, topics, options);

    consumer.on('error', (err) => {
        console.log('error', err);
    });

    client.refreshMetadata([topic], (err) => {
        if (err) {
            throw err;
        }

        consumer.on('message', (message) => {
            console.log("message.value: ", message.value);
            handleMessageFunc(message.value);
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
    });

    // TODO: Perhaps return Consumer, so it can be cancelled when the browser closes
}