const mqtt = require("mqtt");
const fs = require("fs");
const path = require("path");
const { app } = require("@azure/functions");
const { DefaultAzureCredential } = require("@azure/identity");
const { BlobServiceClient } = require("@azure/storage-blob");

// Define the app-level HTTP handler
app.http("sendMQTTMessageToDevice", {
  methods: ["POST"],
  authLevel: "anonymous", // Or use 'anonymous' if no authentication is required
  handler: async (request, context) => {
    context.log("Processing MQTT message request.");

    // Define MQTT broker hostname and port
    const hostname = "namespace1.australiaeast-1.ts.eventgrid.azure.net";
    const port = 8883;

    // Define the topic and message to publish
    const { devEUI, data } = await request.json();

    const topic = `milesight/downlink/${devEUI}`;
    // Convert data from hex to base64
    const buffer = Buffer.from(data, "hex");
    const base64Data = buffer.toString("base64");

    const message = JSON.stringify({
      confirmed: true,
      fport: 85,
      data: base64Data,
    });

    console.log(message, topic);

    // Define the client ID and credentials
    const clientId = process.env.MQTT_SENDER_CLIENT_ID;
    const username = "mqttSenderFunctionApp"; // Replace with your actual username
    const password = ""; // Replace with your password if necessary (empty if none)

    // Load the client certificate and key files
    const credential = new DefaultAzureCredential();
    const blobServiceClient = new BlobServiceClient(
      `https://ug65data7284798.blob.core.windows.net`,
      credential
    );
    const containerClient =
      blobServiceClient.getContainerClient("certificates");

    async function getBlobContent(blobName) {
      const blobClient = containerClient.getBlobClient(blobName);
      const downloadBlockBlobResponse = await blobClient.download();
      return (
        await streamToBuffer(downloadBlockBlobResponse.readableStreamBody)
      ).toString();
    }

    async function streamToBuffer(readableStream) {
      return new Promise((resolve, reject) => {
        const chunks = [];
        readableStream.on("data", (data) => {
          chunks.push(data instanceof Buffer ? data : Buffer.from(data));
        });
        readableStream.on("end", () => {
          resolve(Buffer.concat(chunks));
        });
        readableStream.on("error", reject);
      });
    }

    const cert = await getBlobContent("mqttSenderFunctionApp-authn-ID.pem");
    const key = await getBlobContent("mqttSenderFunctionApp-authn-ID.key");

    // Create an MQTT client with TLS/SSL options
    const options = {
      clientId: clientId,
      username: username,
      password: password,
      port: port,
      protocol: "mqtts", // Use MQTT over SSL
      rejectUnauthorized: true, // Ensure certificate validation
      key: key,
      cert: cert,
    };

    // Create an MQTT client and connect
    const client = mqtt.connect(`mqtts://${hostname}`, options);

    try {
      await new Promise((resolve, reject) => {
        client.on("connect", () => {
          context.log(`Connected to MQTT broker at ${hostname}`);

          // Publish the message to the specified topic
          client.publish(topic, message, { qos: 1 }, (err) => {
            if (err) {
              context.log("Error while publishing message:", err);
              client.end();
              reject(new Error("Failed to send MQTT message."));
            } else {
              context.log(`Message sent to ${topic}: ${message}`);
              client.end();
              resolve();
            }
          });
        });

        client.on("error", (error) => {
          context.log("Connection error:", error);
          reject(new Error("MQTT connection error."));
        });
      });

      return {
        status: 200,
        body: JSON.stringify({
          message: "MQTT message sent successfully!",
          headers: {
            "Content-Type": "application/json",
          },
          details: {
            topic: topic,
            message: message,
            clientId: clientId,
          },
        }),
      };
    } catch (error) {
      return {
        status: 500,
        body: error.message,
      };
    }
  },
});
