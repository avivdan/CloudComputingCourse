const generateOrder = require("../models/orderProd");
const kafkaProducer = require("../models/kafkaProd");

var count = 1;

function pubOrders(){
    kafkaProducer.publish(generateOrder.generateOrder(), count++)
}
setInterval(pubOrders, 5000);


