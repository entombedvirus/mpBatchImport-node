// USING mixpanel's /import to... import some events to mixpanel ;)
// https://developer.mixpanel.com/reference/events#import-events
// by ak@mixpanel.com

//DEPENDENCIES
/* beautify ignore:start */
const { readFile } = require('fs');
const { promisify } = require('util');
const readFilePromisified = promisify(readFile); 
require('dotenv').config(); //https://www.npmjs.com/package/dotenv
const fetch = require('node-fetch'); //https://www.npmjs.com/package/node-fetch
const md5 = require('md5'); //https://www.npmjs.com/package/md5
const isGzip = require('is-gzip'); //https://www.npmjs.com/package/is-gzip
const { gzip, ungzip } = require('node-gzip'); //https://www.npmjs.com/package/node-gzip
/* beautify ignore:end */


//CREDENTIALS
const creds = {
    //project to import data into
    project_id: '', //https://help.mixpanel.com/hc/en-us/articles/115004490503-Project-Settings#project-id

    //service account credentials    
    username: '',
    password: '' //https://developer.mixpanel.com/reference/authentication#service-account
}

//note: credentials can also be stored in .env file, like:
/*
PROJECTID=<yourProjectId>
USERNAME=<yourServiceAccount>
PASSWORD=<yourSecret>
*/

//SOURCE DATA
let pathToDataFile = `./someTestData.ndjson` //note: the path to a data file can also be passed in as a command line argument

//CONFIG + LIMITS
const ENDPOINT_URL = `https://api.mixpanel.com/import`
const EVENTS_PER_BATCH = 2000
const BYTES_PER_BATCH = 2 * 1024 * 1024
const lastArgument = [...process.argv].pop()
if (lastArgument.includes('json')) {
    pathToDataFile = lastArgument;
}


async function main(credentials = {}, dataFile) {
    console.log('starting up...\n');

    //AUTH
    //prefer .env credentials, if they exist
    if (process.env.PROJECTID && process.env.USERNAME && process.env.PASSWORD) {
        console.log(`using .env supplied credentials:\n
            project id: ${process.env.PROJECTID}
            user: ${process.env.USERNAME}
        `);

        credentials.project_id = process.env.PROJECTID
        credentials.username = process.env.USERNAME
        credentials.password = process.env.PASSWORD
    } else {
        console.log(`using hardcoded credentials:\n
        project id: ${credentials.project_id}
        user: ${credentials.username}
        `)
    }

    //LOAD
    let file = await readFilePromisified(dataFile).catch((e)=>{
        console.error(`failed to load ${dataFile}... does it exist?\n`);
        console.log(`if you require some test data, try 'npm run generate' first...`);
        process.exit(1);
    });


    //DECOMPRESS
    let decompressed;
    if (isGzip(file)) {
        console.log('unzipping file\n')
        decompressed = await (await ungzip(file)).toString();
    } else {
        decompressed = file.toString();
    }

    //UNIFY
    //if it's already JSON, just use that
    let allData;
    try {
        allData = JSON.parse(decompressed)
    } catch (e) {
        //it's probably NDJSON, so iterate over each line
        try {
            allData = decompressed.split('\n').map(line => JSON.parse(line));
        } catch (e) {
            //if we don't have JSON or NDJSON... fail...
            console.log('failed to parse data... only valid JSON or NDJSON is supported by this script')
            console.log(e)
        }
    }

    console.log(`parsed ${numberWithCommas(allData.length)} events from ${pathToDataFile}\n`);

    //TRANSFORM
    for (singleEvent of allData) {

        //ensure each event has an $insert_id prop
        if (!singleEvent.properties.$insert_id) {
            let hash = md5(singleEvent);
            singleEvent.properties.$insert_id = hash;
        }

        //ensure each event doesn't have a token prop
        if (singleEvent.properties.token) {
            delete singleEvent.properties.token
        }

        //etc...

        //other checks and transforms go here
        //consider checking for the existince of event name, distinct_id, and time, and max 255 props
        //as per: https://developer.mixpanel.com/reference/events#validation
    }


    //CHUNK

    //max 2000 events per batch
    const batches = chunkForNumOfEvents(allData, EVENTS_PER_BATCH);

    //max 2MB size per batch
    const batchesSized = chunkForSize(batches, BYTES_PER_BATCH);


    //COMPRESS
    const compressed = await compressChunks(batchesSized)


    //FLUSH
    console.log(`sending ${numberWithCommas(allData.length)} events in ${numberWithCommas(batches.length)} batches\n`);
    let numRecordsImported = 0;
    for (eventBatch of compressed) {
        let result = await sendDataToMixpanel(credentials, eventBatch);
        console.log(result);
        numRecordsImported += result.num_records_imported || 0;
    }

    //FINISH
    console.log(`\nsuccessfully imported ${numberWithCommas(numRecordsImported)} events`);
    console.log('finshed.');
    process.exit(0);
}

//HELPERS
function chunkForNumOfEvents(arrayOfEvents, chunkSize) {
    return arrayOfEvents.reduce((resultArray, item, index) => {
        const chunkIndex = Math.floor(index / chunkSize)

        if (!resultArray[chunkIndex]) {
            resultArray[chunkIndex] = [] // start a new chunk
        }

        resultArray[chunkIndex].push(item)

        return resultArray
    }, [])
}

function chunkForSize(arrayOfBatches, maxBytes) {
    return arrayOfBatches.reduce((resultArray, item, index) => {
        //assume each character is a byte
        const currentLengthInBytes = JSON.stringify(item).length

        if (currentLengthInBytes >= maxBytes) {
            //if the batch is too big; cut it in half
            //todo: make this is a little smarter
            let midPointIndex = Math.ceil(item.length / 2);
            let firstHalf = item.slice(0, midPointIndex);
            let secondHalf = item.slice(-midPointIndex);
            resultArray.push(firstHalf);
            resultArray.push(secondHalf);
        } else {
            resultArray.push(item)
        }

        return resultArray
    }, [])
}

async function compressChunks(arrayOfBatches) {
    const allBatches = arrayOfBatches.map(async function (batch) {
        return await gzip(JSON.stringify(batch))
    });
    return Promise.all(allBatches);
}

async function sendDataToMixpanel(auth, batch) {
    let authString = 'Basic ' + Buffer.from(auth.username + ':' + auth.password, 'binary').toString('base64');
    let url = `${ENDPOINT_URL}?project_id=${auth.project_id}&strict=1`
    let options = {
        method: 'POST',
        headers: {
            'Authorization': authString,
            'Content-Type': 'application/json',
            'Content-Encoding': 'gzip'

        },
        body: batch
    }

    try {
        let req = await fetch(url, options);
        let res = await req.json();
        return res;
        //console.log(`${res}\n`)
    } catch (e) {
        console.log(`problem with request:\n${e}`)
    }
}

function numberWithCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

//that's all folks!
main(creds, pathToDataFile);