const config = require("../../environments/index");
const handler = require("./function.handler");
const auth = require("../../services/auth.service");

const Consumer = require("../../services/kafka.service").Consumer;
const Producer = require("../../services/kafka.service").Producer;
Consumer.on('kafkaMessage', async (data) => {
    let message = data.message, topic = data.topic;
    if (topic == `${config.service.name}_create_req`) {
        await create(message)
    }
    else if (topic == `${config.service.name}_read_req`) {
        await read(message)
    }
    else if (topic == `${config.service.name}_update_req`) {
        await update(message)
    }
    else if (topic == `${config.service.name}_delete_req`) {
        await remove(message)
    }
    else if (topic == `${config.service.name}_executeRead_req`) {
        await executeRead(message)
    }
});


async function produce(message, event) {
    if (message.value) {
        message = JSON.parse(message.value.toString());
        let decoded = auth.decode(message.auth);
        message.userId = decoded;
        let req = {
            body: message
        }
        let res = await `handler.${event}(req)`;
        res.data.requestId = message.requestId;
        console.log(res); // 
        //Produce back to kafka
        await Producer.send({
            topic: `${config.service.name}_${event}_res`,
            messages: [
                { key: 'data', value: JSON.stringify(res.data) }
            ],
        })
    }
    return 1;
}


async function errr(err, event) {
    console.log(err);
    let res = {
        status: false
    }
    await Producer.send({
        topic: `${config.service.name}_${event}_res`,
        messages: [
            { key: 'data', value: JSON.stringify(res) }
        ],
    })
}
async function create(message) {
    let event = 'create';
    try {
        produce(message, event);
    }
    catch (err) {
        errr(err, event);
    }
}

async function read(message) {
    let event = 'read';
    try {
        produce(message, event);
    }
    catch (err) {
        errr(err, event);
    }
}

async function executeRead(message) {
    let event = 'execute';
    try {
        produce(message, event);
    }
    catch (err) {
        errr(err, event);
    }
}

async function remove(message) {
    let event = 'delete';
    try {
        produce(message, event);
    }
    catch (err) {
        errr(err, event);
    }
}

async function update(message) {
    let event = 'update';
    try {
        produce(message, event);
    }
    catch (err) {
        errr(err, event);
    }
}