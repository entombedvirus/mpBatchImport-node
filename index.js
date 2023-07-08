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
  token: '',
}

//note: credentials can also be stored in .env file, like:
/*
TOKEN=<yourProjectToken>
*/

//SOURCE DATA
let pathToDataFile = `./someTestData.json` //note: the path to a data file can also be passed in as a command line argument

//CONFIG + LIMITS
const IMPORT_ENDPOINT_URL = `https://api.mixpanel.com/import`
const ENGAGE_ENDPOINT_URL = `https://api.mixpanel.com/engage?verbose=2`
const EVENTS_PER_BATCH = 2000
const PROFILE_UPDATES_PER_BATCH = 2000
const BYTES_PER_BATCH = 2 * 1024 * 1024
const lastArgument = [...process.argv].pop()
if (lastArgument.includes('json')) {
  pathToDataFile = lastArgument;
}


async function main(credentials = {}, dataFile) {
  console.log('starting up...\n');

  //AUTH
  //prefer .env credentials, if they exist
  if (process.env.TOKEN) {
    console.log(`using .env supplied credentials:\n
            project token: ${process.env.TOKEN}
        `);
    credentials.token = process.env.TOKEN
  } else {
    console.log(`using hardcoded credentials:\n
        project token: ${credentials.token}
        `)
  }

  //LOAD
  let file_contents = await readFilePromisified(dataFile).catch((e) => {
    console.error(`failed to load ${dataFile}... does it exist?\n`);
    console.log(`if you require some test data, try 'npm run generate' first...`);
    process.exit(1);
  });


  //DECOMPRESS
  let decompressed;
  if (isGzip(file_contents)) {
    console.log('unzipping file\n')
    decompressed = await (await ungzip(file_contents)).toString();
  } else {
    decompressed = file_contents.toString();
  }

  //UNIFY
  //if it's already JSON, just use that
  let allData;
  try {
    allData = JSON.parse(decompressed)
  } catch (e) {
    //if we don't have JSON or NDJSON... fail...
    console.log('failed to parse data... only valid JSON is supported by this script')
    console.log(e)
  }

  const { profiles, events } = allData;
  console.log(`parsed ${numberWithCommas(events.length)} events and ${numberWithCommas(Object.keys(profiles).length)} profiles from ${pathToDataFile}\n`);

  await Promise.all([
    send_events(events, credentials),
    send_profile_updates(profiles, credentials),
  ])

  //FINISH
  console.log(`\nsuccessfully imported`);
  console.log('finshed.');
  process.exit(0);
}

async function send_profile_updates(profiles, creds) {
  let batch = [];
  let pending_ops = [];
  for (const [distinct_id, sorted_profiles] of Object.entries(profiles)) {
    const sorted_updates = sorted_profiles.map(x => {
      return {
        "token": creds.token,
        "$distinct_id": distinct_id,
        "$set": {
          ...x.profile,
        },
      };
    });
    if (batch.length + sorted_updates.length > PROFILE_UPDATES_PER_BATCH) {
      pending_ops.push(flush_profile_update_batch(batch));
      batch = sorted_updates;
    } else {
      batch = batch.concat(sorted_updates);
    }
  }

  if (batch.length > 0) {
    pending_ops.push(flush_profile_update_batch(batch));
  }

  return await Promise.all(pending_ops);
}

async function flush_profile_update_batch(updates) {
  let options = {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(updates),
  }

  try {
    let req = await fetch(ENGAGE_ENDPOINT_URL, options);
    let res = await req.json();
    console.log(`POST /engage w/ ${updates.length} updates: `, res)
  } catch (e) {
    console.log(`problem with request:\n${e}`)
  }
}

async function send_events(events, credentials) {
  //TRANSFORM
  for (singleEvent of events) {

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
  const batches = chunkForNumOfEvents(events, EVENTS_PER_BATCH);

  //max 2MB size per batch
  const batchesSized = chunkForSize(batches, BYTES_PER_BATCH);


  //COMPRESS
  const compressed = await compressChunks(batchesSized)


  //FLUSH
  console.log(`sending ${numberWithCommas(events.length)} events in ${numberWithCommas(batches.length)} batches\n`);
  let numRecordsImported = 0;
  for (eventBatch of compressed) {
    let result = await sendDataToMixpanel(credentials, eventBatch);
    numRecordsImported += result.num_records_imported || 0;
  }

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
  const allBatches = arrayOfBatches.map(async function(batch) {
    return await gzip(JSON.stringify(batch))
  });
  return Promise.all(allBatches);
}

async function sendDataToMixpanel(auth, batch) {
  let authString = 'Basic ' + Buffer.from(auth.token + ':', 'binary').toString('base64');
  let url = `${IMPORT_ENDPOINT_URL}?strict=1`
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
    console.log(`POST /import: `, res)
    return res;
  } catch (e) {
    console.log(`problem with request:\n${e}`)
  }
}

function numberWithCommas(x) {
  return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

//that's all folks!
if (require.main === module) {
  main(creds, pathToDataFile);
} else {
  console.log('required as a module');
}


module.exports = main
