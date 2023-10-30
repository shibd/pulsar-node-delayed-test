const Pulsar = require('pulsar-client');

(async () => {

    Pulsar.Client.setLogHandler((level, file, line, message) => {
        console.log('[%s][%s:%d] %s', Pulsar.LogLevel.toString(level), file, line, message);
    });

    const client = new Pulsar.Client({
        serviceUrl: 'pulsar://localhost:6650',
        operationTimeoutSeconds: 30,
    });

    const topic = 'persistent://public/default/produce-read-delayed2';
    const producer = await client.createProducer({
        topic,
        sendTimeoutMs: 100,
        batchingEnabled: false,
    });

    let resolveMessageReceived;
    const messageReceived = new Promise((resolve) => {
        resolveMessageReceived = resolve;
    });
    const consumer = await client.subscribe({
        topic,
        subscription: 'sub',
        subscriptionType: 'Shared',
        listener: (msg, msgConsuemr) => {
            console.log(`Received message at ${new Date().toLocaleString()}: ${msg.getData().toString()}`);
            msgConsuemr.acknowledge(msg);
            resolveMessageReceived();
        },
    });

    const sendTime = new Date();
    console.log(`Sending message at ${sendTime.toLocaleString()}`);

    await producer.send({
        data: Buffer.from(`Hello, pulsar, delayed, ${sendTime.toLocaleString()}`),
        deliverAfter: 10000,
    });

    // Wait for the message to be received and processed
    await messageReceived;
    await producer.close();
    await consumer.close();
    await client.close();

})();