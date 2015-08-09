
import net from 'net'

import pg from 'pg'
import sbs1 from 'sbs1'
import moment from 'moment'
import minimist from 'minimist'
import streamSplit from 'split2'
import sql, {insert} from 'sql-bricks'
import {compose, filter, pick, curry, propEq, propIs, head, concat, join, take} from 'ramda'

import {Maybe, map} from './fun'

import 'core-js/fn/map'
import 'core-js/fn/string'

const argv = minimist(process.argv.slice(2), {
  alias: {
    host: ['h'],
    port: ['p'],
    pgurl: ['db', 'd']
  },
  default: {
    host: 'localhost',
    port: 30003,
    pgurl: 'postgres://localhost/adsb'
  }
})

const store = new Map()
let count = 0
let currentHour

pg.connect(argv.pgurl, function (err, client, done) {

  startStream(client, 0)

})

function log (prefix) {
  return function (...message) {
    console.log(prefix, ...message)
  }
}

function startStream (client, attempts) {

  console.log(attempts)

  if ( attempts > 1 ) {
    throw new Error('can’t connect to dump1090')
  }

  console.log('start stream')

  const dump1090Client = net.connect({host: argv.host, port: argv.port})

  dump1090Client.on('connect', function () {

    attempts = 0

    dump1090Client
      .pipe(streamSplit())
      .on('data', compose(map(insertDB(client)), Maybe.of, enhance, sbs1.parseSbs1Message) )

  })

  dump1090Client.on('close', function () {
    startStream(client, attempts+1)
  })

  dump1090Client.on('connect', log('connect') )
  dump1090Client.on('close', log('close') )
  dump1090Client.on('error', log('error') )
  dump1090Client.on('end', log('end') )

  return dump1090Client

}

setInterval(cleanStore(store), 600e3)

function cleanStore (store) {
  return function () {
    let i = 0
    for ( let [key, value] of store ) {
      if ( value && value.expires && moment().isAfter(value.expires) ) {
        store.delete(key)
        i++
      }
    }
    console.log(`${i} entries expired`)
  }
}

// log('parse') map(inlineLog),

function inlineLog (obj) {
  console.log(obj)
  return obj
}

function nullify (val) {
  if ( is(val) ) {
    return val
  }
  return sql('NULL')
}

function generateSql (obj) {
  return insert('decoded', {
    date: nullify(obj.generated),
    icao: nullify(obj.icao),
    callsign: nullify(obj.callsign),
    squawk: nullify(obj.squawk),
    altitude: nullify(obj.altitude),
    groundspeed: nullify(obj.ground_speed),
    track: nullify(obj.track),
    verticalspeed: nullify(obj.vertical_rate),
    geom: sql(`ST_SetSRID(ST_MakePoint(${obj.coordinates.join(', ')}), 4326)`)
  }).toString()
}

function insertDB (client) {
  return function (obj) {

    client.query(generateSql(obj), function (err, res) {

      if (err) {
        console.log(err)
        throw err
      }

      if ( currentHour !== moment().hour() ) {
        currentHour = moment().hour()
        count = 0
      }

      console.log(`${(count++).toString().lpad(6)}: ${obj.generated} - ${obj.icao} ${obj.callsign ? rpad(10, obj.callsign) : rpad(10, '')} ${lpad(6, obj.altitude.toString())} (${coordsLogFormat(obj.coordinates)})`)

    })
  }
}

const toFixed = curry( (n, number) => {
  return number.toFixed(n)
})

const lpad = curry( (n, string) => {
  return string.lpad(n)
})

const rpad = curry( (n, string) => {
  return string.rpad(n)
})

const coordsLogFormat = compose(join(', '), map(lpad(9)), map(toFixed(4)), take(2))

function trim (string) {
  return string.trim()
}

function toNumber (string) {
  return Number(string)
}

function toBoolean (string) {
  return Boolean(string)
}

function is(value) {
  return value !== null && value !== undefined
}

function rateLimited(store, icao, date) {

  const last = store.get(mapKey(icao, 'pos'))
  if ( last && last.expires && moment(last.expires).add(1, 'seconds').isAfter(date) ) {
    return true
  }
  store.set(mapKey(icao, 'pos'), {expires: date.toISOString()})
  return false

}

function enhance (obj) {

  const date = moment.utc(obj.generated_date + ' ' + obj.generated_time, 'YYYY/MM/DD HH:mm:ss.SSS')
  const icao = obj.hex_ident

  const shortTimeout = date.clone().add(10, 'seconds');
  const longTimeout = date.clone().add(30*60, 'seconds');

  if ( is(obj.track) ) {
    setForHex(store, icao, 'track', toNumber(obj.track), shortTimeout)
  }

  if ( is(obj.squawk) ) {
    setForHex(store, icao, 'squawk', toNumber(obj.squawk), longTimeout)
  }

  if ( is(obj.altitude) ) {
    setForHex(store, icao, 'altitude', toNumber(obj.altitude), shortTimeout)
  }

  if ( is(obj.callsign) ) {
    setForHex(store, icao, 'callsign', trim(obj.callsign), longTimeout)
  }

  if ( is(obj.ground_speed) ) {
    setForHex(store, icao, 'ground_speed', toNumber(obj.ground_speed), shortTimeout)
  }

  if ( is(obj.vertical_rate) ) {
    setForHex(store, icao, 'vertical_rate', toNumber(obj.vertical_rate), shortTimeout)
  }

  if ( obj.lat && obj.lon && !rateLimited(store, icao, date) ) {

    return {
      icao: icao,
      generated: date.toISOString(),
      track: getForHex(store, icao, 'track', date),
      squawk: getForHex(store, icao, 'squawk', date),
      altitude: obj.altitude ? obj.altitude : getForHex(store, icao, 'altitude', date),
      callsign: getForHex(store, icao, 'callsign', date),
      ground_speed: getForHex(store, icao, 'ground_speed', date),
      vertical_rate: getForHex(store, icao, 'vertical_rate', date),
      coordinates: [obj.lon, obj.lat, (obj.altitude ? obj.altitude : getForHex(store, icao, 'altitude', date))*0.3048]
    }

  } else {
    return null
  }

}

function mapKey(icao, field) {
  return `${icao}+${field}`;
}

function setForHex(store, icao, key, value, expires) {
  set(store, mapKey(icao, key), value, expires)
}

function getForHex(store, icao, key, date) {
  return get(store, mapKey(icao, key), date)
}

function set (store, key, value, expires) {
  store.set(key, {value: value, expires: expires.toISOString()})
}

function get (store, key, date) {
  const obj = store.get(key)
  if ( obj && date.isBefore(obj.expires) ) {
    return obj.value
  }
  return null
}

