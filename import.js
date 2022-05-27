const fs = require('fs');
const readline = require('readline');
const AWS = require('aws-sdk');

const args = process.argv.slice(2);
if (args.length === 0) {
    console.error('Please provide a DynamoDB JSON file as an argument to this script.');
    process.exit();
}

let recordCount = 0;
let writtenRecords = 0;
let records = [];
const inputFilename = args[0];

const writeBatch = recordBatch => new Promise((resolve, reject) => {
    const dynamodb = new AWS.DynamoDB({
        region: 'us-east-2'
    });

    const params = {
        RequestItems: {
            'Alerts': recordBatch.map(r => {
                return {
                    PutRequest: r
                }
            })
        }
    };

    dynamodb.batchWriteItem(params, (error, data) => {
        if (error) {
            reject(error);
        } else {
            writtenRecords = writtenRecords + recordBatch.length;
            console.log(`${writtenRecords} of ${recordCount} records written.`);
            resolve(data);
        }
    });
});

const processRecords = async () => {
    const rd = readline.createInterface({
        input: fs.createReadStream(inputFilename),
        console: false
    });

    for await (const line of rd) {
        const parsedLine = JSON.parse(line);
        records.push(parsedLine);
        recordCount = recordCount + 1;
    }

    console.log(`Read ${recordCount} records from file.`);

    for (let i = 0; i * 25 < records.length; i++) {
        const startIndex = i * 25;
        const batch = records.slice(startIndex, startIndex + 25);
        await writeBatch(batch);
    }

    console.log(`Operation complete.`);
};

processRecords();