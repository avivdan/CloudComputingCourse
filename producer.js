const Kafka = require('node-rdkafka');
const { configFromPath } = require('./util');
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

function getRandomInt(max) {
  return Math.floor(Math.random() * max);
}

function getBranchName(branch_id) {
  switch (branch_id) {
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
function getAreaName(branch_id) {
  switch (branch_id) {
    case 1:
      return "Haifa";
      break;
    case 2:
      return "Mirkaz";
      break;
    case 3:
      return "Darom";
      break;
    default:
      return "Mizrah";
      break;
  }
}

function getRandomToppings(numberOfTopping) {
  var items = ["olives", " pepper", "mushrooms", "jalapinio", "onion"];
  var newItems = [];

  for (var i = 0; i < numberOfTopping; i++) {
    var idx = Math.floor(Math.random() * items.length);
    newItems.push(items[idx]);
    items.splice(idx, 1);
  }

  return newItems;
}

/**
function create_order() {
  
  let b_id = getRandomInt(4);
  let pizzaobj = {
    "order_id":count++,
    "branch_id": b_id,
    "branch_name": getBranchName(b_id),
    "area": getAreaName(b_id),
    "Date": Date(),
    "status": "proccessing",
    "pizza_topping": getRandomToppings(2)
  };
  async function produceExample() {
    if (process.argv.length < 3) {
      console.log("Please provide the configuration file path as the command line argument");
      process.exit(1);
    }
    let configPath = process.argv.slice(2)[0];
    const config = await configFromPath(configPath);

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

        
    producer.produce(topic, -1, pizzaobj, count);
    
    

    producer.flush(100, () => {
      producer.disconnect();
    });
  }
  console.log(pizzaobj);

setInterval(create_order, 5000);


*/

// function create_order() {
  
  
  async function produceExample() {
    if (process.argv.length < 3) {
      console.log("Please provide the configuration file path as the command line argument");
      process.exit(1);
    }
    let configPath = process.argv.slice(2)[0];
    const config = await configFromPath(configPath);

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
    let b_id = getRandomInt(4);
    let pizzaobj = {
      "order_id":count++,
      "branch_id": b_id,
      "branch_name": getBranchName(b_id),
      "area": getAreaName(b_id),
      "Date": Date(),
      "status": "proccessing",
      "pizza_topping": getRandomToppings(2)
    };
    var a = [pizzaobj["branch_id"]]
    var result = JSON.stringify(pizzaobj);    // Buffer.from();
    result = new Buffer(result);
    producer.produce(topic, -1, result, count);
    
    

    producer.flush(10000, () => {
      producer.disconnect();
    });
    console.log(pizzaobj);
  }
  

setInterval(produceExample, 5000);


// async function produceExample() {
//   if (process.argv.length < 3) {
//     console.log("Please provide the configuration file path as the command line argument");
//     process.exit(1);
//   }
//   let configPath = process.argv.slice(2)[0];
//   const config = await configFromPath(configPath);

//   let topic = "topic_0";

//   let pizzaobj = {
//     "branch_id": b_id,
//     "branch_name": getBranchName(b_id),
//     "area": getAreaName(b_id),
//     "Date": Date(),
//     "status": "proccessing",
//     "pizza_topping": getRandomToppings(2)
//   };
//   // let users = ["eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther"];
//   // let items = ["book", "alarm clock", "t-shirts", "gift card", "batteries"];

//   const producer = await createProducer(config, (err, report) => {
//     if (err) {
//       console.warn('Error producing', err)
//     } else {
//       const { topic, key, value } = report;
//       let k = key.toString().padEnd(10, ' ');
//       console.log(`Produced event to topic ${topic}: key = ${k} value = ${value}`);
//     }
//   });

//   let numEvents = 10;
//   for (let idx = 0; idx < numEvents; ++idx) {

//     const key = users[Math.floor(Math.random() * users.length)];
//     const value = Buffer.from(items[Math.floor(Math.random() * items.length)]);

//     producer.produce(topic, -1, pizzaobj, key);
//   }
//   const key = pizzaobj["branch_id"];
//   producer.produce(topic, -1, pizzaobj, key);

//   producer.flush(10000, () => {
//     producer.disconnect();
//   });
// }