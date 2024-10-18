const { app } = require("@azure/functions");
const { TableClient } = require("@azure/data-tables");

app.http("getSensorData-azFunction", {
  methods: ["GET", "POST"],
  authLevel: "anonymous",
  handler: async (request, context) => {
    context.log("JavaScript HTTP trigger function processed a request.");

    const connectionString = process.env.TABLE_STORAGE_CONNECTION_STRING;
    const tableName = "sensorData";
    const query = request.query.get("query");

    if (query == null) {
      return {
        status: 500,
        body: "Query parameter is required.",
      };
    }

    const tableClient = TableClient.fromConnectionString(connectionString, tableName);

    try {
      const entities = [];
      for await (const entity of tableClient.listEntities({
        queryOptions: { filter: query }
      })) {
        ['partitionKey', 'rowKey', 'timestamp'].forEach(key => {
            entity[key.charAt(0).toUpperCase() + key.slice(1)] = entity[key];
            delete entity[key];
        });
        entities.push(entity);
      }

      return {
        status: 200,
        body: JSON.stringify(entities),
      };
    } catch (error) {
      context.log("Error fetching data from Table Storage:", error.message);

      return {
        status: 500,
        body: "An error occurred while fetching data.",
      };
    }
  },
});