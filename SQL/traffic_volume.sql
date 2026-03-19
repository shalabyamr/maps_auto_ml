DROP TABLE IF EXISTS PUBLIC.FACT_TRAFFIC_VOLUME;
CREATE TABLE PUBLIC.FACT_TRAFFIC_VOLUME AS (
    SELECT
      _id
    , latest_count_id as location_id
    , location_name as location
    , longitude as lng
    , latitude as lat
    , centreline_type
    , centreline_id
    , px
    , count_date
    , download_link
    , src_filename
    , last_updated
    , (NOW() AT TIME ZONE 'EST') AS last_inserted
    FROM(
      SELECT *, ROW_NUMBER() OVER(PARTITION BY latest_count_id ORDER BY count_date DESC) RN
      FROM STAGE.stg_traffic_volume
   ) A WHERE RN=1);

