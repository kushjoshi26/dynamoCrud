"use strict";

require("dotenv").config();

var AWS = require("aws-sdk");

const my_AWSAccessKeyId = process.env.AWSAccessKeyId;
const my_AWSSecretKey = process.env.AWSSecretKey;
const aws_region = process.env.region;
const empTable = process.env.tableName;

var dynamoDB = new AWS.DynamoDB.DocumentClient({
  region: "localhost",
  endpoint: "http://localhost:8000",
  accessKeyId: my_AWSAccessKeyId,
  secretAccessKey: my_AWSSecretKey,
});

async function createTable() {
  try {
    // Load the AWS SDK for Node.js
    // Create the DynamoDB service object

    var params = {
      KeySchema: [
        {
          AttributeName: "id",
          KeyType: "HASH",
        },
        {
          AttributeName: "dept",
          KeyType: "RANGE",
        },
      ],
      AttributeDefinitions: [
        {
          AttributeName: "id",
          AttributeType: "S",
        },
        {
          AttributeName: "dept",
          AttributeType: "S",
        },
      ],

      GlobalSecondaryIndexes: [
        // optional (list of GlobalSecondaryIndex)
        {
          IndexName: "dept_index",
          KeySchema: [
            {
              // Required HASH type attribute
              AttributeName: "dept",
              KeyType: "HASH",
            },
          ],
          Projection: {
            // attributes to project into the index
            ProjectionType: "ALL", // (ALL | KEYS_ONLY | INCLUDE)
          },
          ProvisionedThroughput: {
            // throughput to provision to the index
            ReadCapacityUnits: 400,
            WriteCapacityUnits: 400,
          },
        },
        // ... more global secondary indexes ...
      ],

      ProvisionedThroughput: {
        ReadCapacityUnits: 1,
        WriteCapacityUnits: 1,
      },
      TableName: empTable,
      StreamSpecification: {
        StreamEnabled: false,
      },
    };

    dynamoDB.createTable(params, function (err, data) {
      if (err) {
        console.log("Error", err);
      } else {
        console.log("Table Created", data);
      }
    });
  } catch (err) {
    console.log("Error", err);
  }
}

async function deleteTable() {
  var params = {
    TableName: empTable,
  };
  dynamoDB.deleteTable(params, function (err, data) {
    if (err && err.code === "ResourceNotFoundException") {
      console.log("Error: Table not found");
    } else if (err && err.code === "ResourceInUseException") {
      console.log("Error: Table in use");
    } else {
      console.log("Success", data);
    }
  });
}

async function insertDataintoDatabase() {
  try {
    var params = {
      TableName: empTable,
      Item: {
        id: "ID_" + Math.random(),
        name: Math.random().toString(36).slice(2, 7),
        client: "client_" + Math.random(),
        dept: "sales",
        status: true,
      },
    };

    let putItem = new Promise((res, rej) => {
      dynamoDB.put(params, function (err, data) {
        if (err) {
          console.log("Error", err);
          rej(err);
        } else {
          console.log("Success!");
          res("Inserted data into Dynamodb!");
        }
      });
    });
    const result = await putItem;
    console.log(result);
  } catch (err) {
    console.log("Error", err);
  }
}

async function fetchDatafromDatabase1() {
  var params = {
    TableName: empTable,
  };

  let queryExecute = new Promise((res, rej) => {
    dynamoDB.scan(params, function (err, data) {
      if (err) {
        console.log("Error", err);
        rej(err);
      } else {
        console.log("Success! Scan method fetch data from dynamodb");
        res(JSON.stringify(data, null, 2));
      }
    });
  });
  const result = await queryExecute;
  console.log(result);
}

async function fetchDatafromDatabase2() {
  var id = "ANPL2032231113";
  var params = {
    TableName: empTable,
    Key: {
      id: id,
    },
  };

  let queryExecute = new Promise((res, rej) => {
    dynamoDB.get(params, function (err, data) {
      if (err) {
        console.log("Error", err);
        rej(err);
      } else {
        console.log("Success! get method fetch data from dynamodb");
        res(JSON.stringify(data, null, 2));
      }
    });
  });
  const result = await queryExecute;
  console.log(result);
}

async function fetchDatafromDatabase3() {
  // query method fetch data from dynamodb
  var id = "ANPL2032231213";
  var Dept = "Sales";
  var params = {
    TableName: empTable,
    KeyConditionExpression: "#id = :id",
    ExpressionAttributeNames: {
      "#id": "id",
      "#dept": "Dept",
    },
    ExpressionAttributeValues: {
      ":id": id,
      ":deptValue": Dept,
    },
    FilterExpression: "#dept = :deptValue", //AttributeName with attributeValue
    Limit: 5,
    ScanIndexForward: false, // Set ScanIndexForward to false to display most recent entries first
  };

  let queryExecute = new Promise((res, rej) => {
    dynamoDB.query(params, function (err, data) {
      if (err) {
        console.log("Error", err);
        rej(err);
      } else {
        console.log("Success! query method fetch data from dynamodb");
        res(JSON.stringify(data, null, 2));
      }
    });
  });
  const result = await queryExecute;
  console.log(result);
}

async function fetchDataUsingGlobalIndex() {
  var params = {
    TableName: empTable,
    IndexName: "dept_index",
    KeyConditionExpression: "dept = :deptVal", // conditional on key
    // ExpressionAttributeNames: {
    //   // column name
    //   "#status": "status",
    // },
    ExpressionAttributeValues: {
      // find values that are added in query
      ":deptVal": "Sales",
      //   ":statusVal": true,
    },
    // FilterExpression: "#status = :statusVal", // added more condition apart form key condition
    // ScanIndexForward: false,
  };

  dynamoDB.query(params, function (err, data) {
    if (err) {
      console.error(
        "Unable to read item. Error JSON:",
        JSON.stringify(err, null, 2)
      );
    } else {
      console.log("GetItem succeeded:", JSON.stringify(data, null, 2));
    }
  });
}

async function updateDatafromDatabase() {
  // update method fetch data from dynamodb
  var id = "ID_0.0004635846388489906";
  const dept_index = "Sales";
  var params = {
    TableName: empTable,
    Key: {
      id: id,
      dept: dept_index,
    },
    UpdateExpression: "set #updateKey = :r",
    ExpressionAttributeValues: {
      ":r": "sales",
    },
    ExpressionAttributeNames: {
      "#updateKey": "sales",
    },
    ReturnValues: "UPDATED_NEW",
  };

  let queryExecute = new Promise((res, rej) => {
    dynamoDB.update(params, function (err, data) {
      if (err) {
        console.log("Error", err);
        rej(err);
      } else {
        console.log("Updated Successfully done for :" + id);
        res(JSON.stringify(data, null, 2));
      }
    });
  });
  const result = await queryExecute;
  console.log(result);
}

async function deleteDatafromDatabase() {
  // delete method fetch data from dynamodb
  var id = "Sales";
  var params = {
    TableName: empTable,
    Key: {
      id: "ID_0.6445619042253641",
      dept: "Sales",
    },
  };

  let queryExecute = new Promise((res, rej) => {
    dynamoDB.delete(params, function (err, data) {
      if (err) {
        console.log("Error", err);
        rej(err);
      } else {
        console.log("Deleted Successfully user :" + id);
        res(JSON.stringify(data, null, 2));
      }
    });
  });
  const result = await queryExecute;
  console.log(result);
}

// createTable()
// deleteTable()

insertDataintoDatabase(); //Insert data into dynamodb
// fetchDatafromDatabase1(); // Scan method fetch data from dynamodb
// fetchDatafromDatabase2(); // Get method fetch data from dynamodb
// fetchDatafromDatabase3(); // Query method fetch data from dynamodb
// updateDatafromDatabase(); // update data from the table
// deleteDatafromDatabase(); // delete data from perticular table
// fetchDataUsingGlobalIndex();
