# dump1090topg

Inserts the SBS1 output of dump1090 into a postgis database.

(Also hydrates the position reports with callsign, track, vertical speed etc. from non-position reports.)

## Usage

```
npm start
```
or
```
babel-node log.js
```
or
```
npm run build
node build/log.js
```

### Options

```
node build/log.js {options}

--host, -h  [default: localhost]
  hostname or ip of the dump1090 instance
  
--port, -p  [default: 30003]
  port the dump1090 instance
  
--pgurl, --db, -d [default: postgres://localhost/adsb]
  url to the postgres/postgis database

--range, -r [default: 500]
  range in km above which to discard reports (needs --lon and --lat)
  
--lon
  Longitude of the receiver (required for --range)

--lat
  Latitude of the receiver (required for --range)
```
