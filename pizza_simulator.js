const fs = require('fs');

function readConfigFile(fileName) {
    const data = fs.readFileSync(fileName, 'utf8').toString().split("\n");
    return data.reduce((config, line) => {
        const [key, value] = line.split("=");
        if (key && value) {
            config[key] = value;
        }
        return config;
    }, {})
}

const Kafka = require("node-rdkafka");
const producer = new Kafka.Producer(readConfigFile("client.properties"));
producer.connect();
producer.on("ready", () => {
    producer.produce("topic_0", -1, Buffer.from("value"), Buffer.from("key"));
});

// const {MongoClient} = require('mongodb');
// const { Kafka } = require('kafkajs');

// const kafka = new Kafka({
//   clientId: 'my-app',
//   brokers: ['pkc-ymrq7.us-east-2.aws.confluent.cloud:9092'],
// })

// const producer = kafka.producer()

// producer.connect()
// producer.send({
//   topic: 'test-topic',
//   messages: [
//     { value: 'Hello KafkaJS user!' },
//   ],
// })

// producer.disconnect()

function getRandomInt(max) {
    return Math.floor(Math.random() * max);
}

function getBranchName(branch_id) {
    switch (branch_id){
        case 1:
            return "papa_jones";
            break;
        case 2:
            return "dominos";
            break;
        case 3:
            return "hat";
            break;
        default:
            return "sod";
            break;
        }       
}

function getRandomToppings(numberOfTopping){
    var items = ["olives"," pepper", "mushrooms", "jalapinio", "onion"];
    var newItems = [];

    for (var i = 0; i < numberOfTopping; i++) {
        var idx = Math.floor(Math.random() * items.length);
        newItems.push(items[idx]);
        items.splice(idx, 1);
    }

    return newItems;
}


function create_order() {
    let time = {
        "transation_id":4,
        "branch_id":getRandomInt(4),
        "branch_name": getBranchName(branch_id),
        "area" : branch_id,
        "Date": Date(),
        "status": "proccessing",
        "pizza_topping": getRandomToppings(2)
    };

    console.log(time)
}

let display = create_order(showTime, 5000);