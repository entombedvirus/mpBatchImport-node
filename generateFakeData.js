const util = require('util');
const fs = require('fs')
const path = require('path')
const Chance = require('chance'); //https://github.com/chancejs/chancejs
const chance = new Chance();
const readline = require('readline');

//possible names of events for test data
const eventNames = ['app open', 'log in', 'send message', 'receive message', 'roll dice', 'attack', 'defend', 'level up', 'start game']

const numUniqueUsers = 100

//time stuffs
const now = Date.now();
const dayInMs = 8.64e+7;

function main() {
  const arrOfEvents = [];
  let numOfEvents = 10000
  const lastArgument = [...process.argv].pop()
  if (!isNaN(lastArgument)) {
    numOfEvents = Number(lastArgument);
  }

  const users = [...Array(numUniqueUsers).keys()].map(_ => chance.guid())
  console.log(`starting data generator with ${users.length} unique users...\n`);

  //mixin for generating random events
  chance.mixin({
    'event': function () {
      const did = chance.pickone(users)
      const profile = addPrefix('$mp_profile:', chance.user_profile(did))

      return {
        event: chance.pickone(eventNames),
        properties: {
          distinct_id: did,
          time: chance.integer({
            min: now - dayInMs * 90, //90 days in the past
            max: now
          }),
          $source: "roh import",
          version: 2,
          luckyNumber: chance.prime({min: 1, max: 10000}),
          ip: chance.ip(),
          email: chance.email(),
          ...profile
        }
      };
    },
    'user_profile': function() {
      return {
        "Driver Tier": chance.pickone(['silver', 'gold', 'diamond', 'platinum']),
        "Driver Battery": chance.integer({min: 0, max: 100}),
        "Driver Ratings": chance.floating({min: 1.0, max: 5.0, fixed: 1}),
        "Driver Preference": chance.pickone(['Food', 'People']),
        "City": chance.city(),

      };
    }
  });

  console.log(`generating ${numberWithCommas(numOfEvents)} events...\n`);

  for (let index = 1; index < numOfEvents+1; index++) {
    arrOfEvents.push(chance.event());
    showProgress('events', index)
  }

  console.log(`\n\nsaving ${numberWithCommas(numOfEvents)} events to ./someTestData.json\n`);

  fs.writeFile("./someTestData.json", JSON.stringify(arrOfEvents), function(err) {
    if(err) {
      return console.log(err);
      process.exit(1)
    }
    console.log("all finished\ntry 'npm run import' to send the data to mixpanel!");
    process.exit(0)
  });

}

//helpers
function numberWithCommas(x) {
  return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function showProgress(thing, p) {
  //readline.clearLine(process.stdout);
  readline.cursorTo(process.stdout, 0);
  process.stdout.write(`${thing} created: ${numberWithCommas(p)}`);
}

function addPrefix(prefix, obj) {
  let ret = {}
  for (const [key, value] of Object.entries(obj)) {
    ret[`${prefix}${key}`] = value
  }
  return ret
}

// ;)
main();
