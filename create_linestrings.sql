DROP FUNCTION IF EXISTS group_messages(interval);

CREATE OR REPLACE FUNCTION group_messages(
  IN max_time_gap INTERVAL
)
RETURNS TABLE(
  out_icao VARCHAR(6),
  out_callsign VARCHAR(10),
  out_date TIMESTAMP WITHOUT TIME ZONE,
  out_line_id INTEGER,
  out_geom GEOMETRY
) AS $$
DECLARE
  plane RECORD;
  report RECORD;
  last_ts TIMESTAMP;
  lid INTEGER;
BEGIN

  lid := -1;

  FOR plane IN
    SELECT DISTINCT icao FROM decoded
  LOOP

    lid := lid + 1;

    last_ts := NULL;

    FOR report IN
      SELECT icao, callsign, date, geom FROM decoded WHERE icao = plane.icao ORDER BY date ASC
    LOOP

      IF last_ts IS NULL THEN
        last_ts := report.date;
      ELSIF report.date - last_ts >= max_time_gap THEN
        lid := lid + 1;
      END IF;

      last_ts := report.date;

      out_icao := report.icao;
      out_callsign := report.callsign;
      out_date := report.date;
      out_line_id := lid;
      out_geom := report.geom;

      RETURN NEXT;

    END LOOP;

  END LOOP;

END;
$$ LANGUAGE plpgsql;

DROP TABLE IF EXISTS lines CASCADE;

CREATE TABLE lines AS
SELECT
  MIN(out_icao) as icao,
  MAX(out_callsign) as callsign,
  MIN(out_date) as start,
  MAX(out_date) as end,
  ST_Makeline(out_geom) as geom
FROM group_messages( '60 seconds'::INTERVAL )
GROUP BY out_line_id
ORDER BY "end";
