const util = require('util');
const fs = require('fs')
const path = require('path')
const Chance = require('chance'); //https://github.com/chancejs/chancejs
const chance = new Chance();
const readline = require('readline');
const config = require('./config');

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
    'event': function(profiles) {
      const did = chance.pickone(users)
      const time = chance.integer({
        min: now - dayInMs * 90, //90 days in the past
        max: now,
      })
      let did_profiles = profiles[did] || []
      if (did_profiles.length == 0) {
        profiles[did] = did_profiles
      }
      if (did_profiles.length == 0 || chance.integer({ min: 1, max: 100 }) <= config.profile_regen_change_percent) {
        did_profiles.push({ time: time, profile: chance.user_profile() })
      }
      const profile = addPrefix(config.profile_prop_prefix, did_profiles.slice(-1)[0].profile)

      return {
        event: chance.pickone(eventNames),
        properties: {
          distinct_id: did,
          time: time,
          $source: "roh import",
          version: 2,
          luckyNumber: chance.prime({ min: 1, max: 10000 }),
          ip: chance.ip(),
          email: chance.email(),
          ...profile
        }
      };
    },
    'user_profile': function() {
      return {
        "Driver Tier": chance.pickone(['silver', 'gold', 'diamond', 'platinum']),
        "Driver Battery": chance.integer({ min: 0, max: 100 }),
        "Driver Ratings": chance.floating({ min: 1.0, max: 5.0, fixed: 1 }),
        "Driver Preference": chance.pickone(['Food', 'People']),
        "City": chance.city(),

      };
    }
  });

  console.log(`generating ${numberWithCommas(numOfEvents)} events...\n`);

  let profiles = {}
  for (let index = 1; index < numOfEvents + 1; index++) {
    arrOfEvents.push(chance.event(profiles));
    showProgress('events', index)
  }

  console.log(`\ngenerated ${numberWithCommas(Object.keys(profiles).length)} profiles...\n`);
  console.log(`\n\nsaving ${numberWithCommas(numOfEvents)} events to ./someTestData.json\n`);

  const contents = {
    profiles: profiles,
    events: arrOfEvents,
  }
  fs.writeFile("./someTestData.json", JSON.stringify(contents), function(err) {
    if (err) {
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
