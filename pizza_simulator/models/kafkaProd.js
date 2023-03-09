const Kafka = require('node-rdkafka');
const { configFromPath } = require('../util');
var count = 1;

function createConfigMap(config) {
  if (config.hasOwnProperty('security.protocol')) {
    return {
      'bootstrap.servers': config['bootstrap.servers'],
      'sasl.username': config['sasl.username'],
      'sasl.password': config['sasl.password'],
      'security.protocol': config['security.protocol'],
      'sasl.mechanisms': config['sasl.mechanisms'],
      'dr_msg_cb': true
    }
  } else {
    return {
      'bootstrap.servers': config['bootstrap.servers'],
      'dr_msg_cb': true
    }
  }
}

function createProducer(config, onDeliveryReport) {

  const producer = new Kafka.Producer(createConfigMap(config));

  return new Promise((resolve, reject) => {
    producer
      .on('ready', () => resolve(producer))
      .on('delivery-report', onDeliveryReport)
      .on('event.error', (err) => {
        console.warn('event.error', err);
        reject(err);
      });
    producer.connect();
  });
}

async function publish(data, count) {
    let configPath = "client.properties";
    const config = await configFromPath(configPath);

    //topic configuration of kafka confluent
    let topic = "topic_0";

    const producer = await createProducer(config, (err, report) => {
      if (err) {
        console.warn('Error producing', err)
      } else {
        const { topic, key, value } = report;
        let k = key.toString().padEnd(10, ' ');
        console.log(`Produced event to topic ${topic}: key = ${k} value = ${value}`);
      }
    });
    
    var result = new Buffer.from(JSON.stringify(data));   
    producer.produce(topic, -1, result, count);
    producer.flush(10000, () => {
      producer.disconnect();
    });
    // console.log(result);
  }
  
module.exports = { publish }
