// const fs = require('fs');
// const Kafka = require("node-rdkafka");
// const config = readConfigFile("client.properties");
// config["group.id"] = "node-group";


// function readConfigFile(fileName) {
//     const data = fs.readFileSync(fileName, 'utf8').toString().split("\n");
//     return data.reduce((config, line) => {
//         const [key, value] = line.split("=");
//         if (key && value) {
//             config[key] = value;
//         }
//         return config;
//     }, {})
// }

// const consumer = new Kafka.KafkaConsumer(config, {"auto.offset.reset": "earliest" });
// consumer.connect();
// consumer.on("ready", () => {
//     consumer.subscribe(["topic_0"]);
//     consumer.consume();
//     console.log("listening on")
// }).on("data", (message) => {
//     console.log("Consumed message", message);
// });
// const { Kafka } = require('kafkajs');

// const kafka = new Kafka({
//   clientId: 'my-app',
//   brokers: ['pkc-ymrq7.us-east-2.aws.confluent.cloud:9092'],
// })


// const consumer = kafka.consumer({ groupId: 'test-group' })

// await consumer.connect()
// await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })

// await consumer.run({
//   eachMessage: async ({ topic, partition, message }) => {
//     console.log({
//             value: message.value.toString(),
//         })
//   },
// })

const Kafka = require('node-rdkafka');
const { configFromPath } = require('./util');

function createConfigMap(config) {
  if (config.hasOwnProperty('security.protocol')) {
    return {
      'bootstrap.servers': config['bootstrap.servers'],
      'sasl.username': config['sasl.username'],
      'sasl.password': config['sasl.password'],
      'security.protocol': config['security.protocol'],
      'sasl.mechanisms': config['sasl.mechanisms'],
      'group.id': 'kafka-nodejs-getting-started'
    }
  } else {
    return {
      'bootstrap.servers': config['bootstrap.servers'],
      'group.id': 'kafka-nodejs-getting-started'
    }
  }
}

function createConsumer(config, onData) {
  const consumer = new Kafka.KafkaConsumer(
      createConfigMap(config),
      {'auto.offset.reset': 'earliest'});

  return new Promise((resolve, reject) => {
    consumer
     .on('ready', () => resolve(consumer))
     .on('data', onData);

    consumer.connect();
  });
};


async function consumerExample() {
  if (process.argv.length < 3) {
    console.log("Please provide the configuration file path as the command line argument");
    process.exit(1);
  }
  let configPath = process.argv.slice(2)[0];
  const config = await configFromPath(configPath);

  //let seen = 0;
  let topic = "topic_0";

  const consumer = await createConsumer(config, ({key, value}) => {
    let k = key.toString().padEnd(10, ' ');
    console.log(`Consumed event from topic ${topic}: key = ${k} value = ${value}`);
  });

  consumer.subscribe([topic]);
  consumer.consume();

  process.on('SIGINT', () => {
    console.log('\nDisconnecting consumer ...');
    consumer.disconnect();
  });
}

consumerExample()
  .catch((err) => {
    console.error(`Something went wrong:\n${err}`);
    process.exit(1);
  });