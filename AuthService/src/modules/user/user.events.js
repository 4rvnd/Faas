const controller = require("./user.controller");
const config = require("../../environments/index");
const handler = require("./user.handler");
const auth = require("../../services/auth.service");

const Consumer = require("../../services/kafka.service").Consumer;
const Producer = require("../../services/kafka.service").Producer;
Consumer.on('kafkaMessage', async (data) => {
    let message = data.message, topic = data.topic;
    if (topic == `${config.service.name}_register_req`) {
        await register(message)
    }
    else if (topic == `${config.service.name}_login_req`) {
        await login(message)
    }
    else if (topic == `${config.service.name}_update_req`) {
        await update(message)
    }
    else if (topic == `${config.service.name}_get_req`) {
        await get(message)
    }
});

async function produce(message, event) {
    if (message.value) {
        message = JSON.parse(message.value.toString());
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


async function register(message) {
    let event = 'register';
    try {
        produce(message, event);
    }
    catch (err) {
        errr(err, event);
    }
}

async function login(message) {
    let event = 'login';
    try {
        produce(message, event);
    }
    catch (err) {
        errr(err, event);
    }
}

async function get(message) {
    let event = 'get';
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