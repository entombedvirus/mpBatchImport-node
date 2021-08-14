// USING mixpanel's /import to... import some events to mixpanel ;)
// https://developer.mixpanel.com/reference/events#import-events
// by AK ... ak@mixpanel.com

//DEPENDENCIES
const fs = require('fs');
const util = require('util');
require('dotenv').config(); //https://www.npmjs.com/package/dotenv
const fetch = require('node-fetch'); //https://www.npmjs.com/package/node-fetch
const md5 = require('md5'); //https://www.npmjs.com/package/md5
const isGzip = require('is-gzip'); //https://www.npmjs.com/package/is-gzip
const {
    gzip,
    ungzip
} = require('node-gzip'); //https://www.npmjs.com/package/node-gzip

//promisfy readFile()
//https://stackoverflow.com/a/46867579/4808195
const readFile = util.promisify(fs.readFile);


//CREDENTIALS
const creds = {
    project_id: '', //https://help.mixpanel.com/hc/en-us/articles/115004490503-Project-Settings#project-id

    //service account credentials
    //https://developer.mixpanel.com/reference/authentication#service-account
    username: '',
    password: ''
}

//note: credentials can also be stored in .env file, like:
/*
PROJECTID=<yourProjectId>
USERNAME=<yourServiceAccount>
PASSWORD=<yourSecret>
*/

//DATA FILE
const pathToDataFile = `./someTestData.ndjson`


const ENDPOINT_URL = `https://api.mixpanel.com/import`

//limits
const EVENTS_PER_BATCH = 2000
const BYTES_PER_BATCH = 2 * 1024 * 1024


async function main(credentials = {}, dataFile) {
    console.log('starting up...\n');

    //CREDS
    //prefer .env credentials, if they exist
    if (process.env.PROJECTID && process.env.USERNAME && process.env.PASSWORD) {
        console.log(`using .env supplied credentials:
            project id: ${process.env.PROJECTID}
            user: ${process.env.USERNAME}
        `);

        credentials.project_id = process.env.PROJECTID
        credentials.username = process.env.USERNAME
        credentials.password = process.env.PASSWORD
    } else {
        console.log(`using hardcoded credentials:
        project id: ${credentials.project_id}
        user: ${credentials.username}
        `)
    }

    //LOAD
    let file = await readFile(dataFile);

    //DECOMPRESS
    let decompressed;
    if (isGzip(file)) {
        console.log('unzipping file')
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

    console.log(`parsed ${numberWithCommas(allData.length)} events\n`);

    //TRANSFORM
    for (singleEvent of allData) {

        //ensure every event has an $insert_id
        if (!singleEvent.properties.$insert_id) {
            let hash = md5(singleEvent);
            singleEvent.properties.$insert_id = hash;
        }

        //ensure every event doesn't have a token
        if (singleEvent.properties.token) {
            delete singleEvent.properties.token
        }

        //etc...

        //other checks and transforms go here
        //consider checking for the existince of event name, distinct_id, and time
        //as per: https://developer.mixpanel.com/reference/events#validation
    }



    //CHUNK

    //chunk for # of events; max 2000
    const batches = chunkForNumOfEvents(allData, EVENTS_PER_BATCH);


    //chunk for size of each batch; max 2MB
    //todo

    //COMPRESS
    const compressed = await compressChunks(batches)

    
    //FLUSH
    console.log(`sending ${numberWithCommas(allData.length)} events in ${numberWithCommas(batches.length)} batches\n`);
    let numRecordsImported = 0;
    for (eventBatch of compressed) {
        let result = await sendDataToMixpanel(credentials, eventBatch);
        console.log(result);
        numRecordsImported += result.num_records_imported || 0;
    }

    console.log(`\nsuccessfully imported ${numberWithCommas(numRecordsImported)} events`);
    console.log('finshed.')
}

//helpers
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