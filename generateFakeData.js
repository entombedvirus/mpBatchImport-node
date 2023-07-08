const util = require('util');
const fs = require('fs')
const path = require('path')
const Chance = require('chance'); //https://github.com/chancejs/chancejs
const chance = new Chance();
const readline = require('readline');
const config = require('./config');
const assert = require('assert');

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
        min: now - dayInMs * config.num_days_in_past,
        max: now,
      })
      let did_profiles = profiles[did]
      assert(did_profiles.length > 0);
      const prefixed_profile_props = addPrefix(
        config.profile_prop_prefix,
        (function() {
          let bigger_idx = did_profiles.findIndex(x => x.profile.time > time);
          if (bigger_idx === 0) {
            // even the first people update happened after this event
            // so this event doesn't get any people props stamped on it
            return {};
          } else if (bigger_idx == -1) {
            // no profile updates happened after this event
            // pick the last update to stamp
            return did_profiles.slice(-1).pop().profile;
          } else {
            // found an update that happened after this event
            // pick the previous update to stamp
            return did_profiles[bigger_idx - 1].profile;
          }
        })()
      )

      return {
        event: chance.pickone(eventNames),
        properties: {
          distinct_id: did,
          time: time,
          $source: "roh import",
          version: config.version,
          luckyNumber: chance.prime({ min: 1, max: 10000 }),
          ip: chance.ip(),
          email: chance.email(),
          ...prefixed_profile_props
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
        "version": config.version,
      };
    }
  });

  console.log(`generating ${numberWithCommas(config.num_profile_updates)} profile updates...`);
  let profiles = {};
  [...Array(config.num_profile_updates).keys()]
    .forEach(idx => {
      const distinct_id = chance.pickone(users)
      const did_profiles = profiles[distinct_id] ||= []
      const time = chance.integer({
        min: now - dayInMs * config.num_days_in_past,
        max: now,
      })
      did_profiles.push({ time: time, profile: chance.user_profile() })
      showProgress('profiles created', idx + 1)
    });

  console.log(`\nsorting profile updates by time...`);
  Object.values(profiles).forEach((did_profiles, idx) => {
    did_profiles.sort((a, b) => a.time - b.time);
    showProgress('profile sorted', idx + 1);
  })

  console.log(`\n\ngenerating ${numberWithCommas(numOfEvents)} events...`);
  for (let index = 1; index < numOfEvents + 1; index++) {
    arrOfEvents.push(chance.event(profiles));
    showProgress('events created', index)
  }

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
  process.stdout.write(`${thing}: ${numberWithCommas(p)}`);
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
