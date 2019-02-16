-- This query exists as an 'all purpose' join query when it comes to artist-credits. 
-- First, extract the releases, tracks, and other metatypes with the artist_credit. Then
-- you can combine this dataset with that dataset and enjoy.
-- Execution outside of python:
-- You can run this file directly is postgresql with the CLI like so:
-- $ psql musicbrainz_db -t -f extract-artists.sql > t
-- You can then inspect the file, assuming that each line is one entry.
-- $ cat t |head -n 1 |tail -n 1|python -m json.tool > tt.json # pretty printing
WITH artist_select AS (
  SELECT
    artist.name AS name,
    artist.gid AS guid,
    artist.id AS artist_id,
    artist.sort_name AS sort_name,
    artist_type.name AS type,
    gender.name AS gender,
    artist_ipi.ipi AS ipi,
    artist_isni.isni AS isni
  FROM artist
  LEFT JOIN artist_type ON (artist.type = artist_type.id)
  LEFT JOIN gender ON (artist.gender = gender.id)
  LEFT JOIN artist_ipi ON (artist.id = artist_ipi.artist)
  LEFT JOIN artist_isni ON (artist.id = artist_isni.artist)
  -- Removed Artist-Credit Query mainly because it excluded some
  --  entries that have links, while they don't have credited releases.
  -- For example, 13 Saints[fc842bfb-7d93-4d21-909d-02fb8d9a8afb] was omitted
  --  from the original query.

  -- Infected Mushroom GUID
  --WHERE artist.gid IN ('eab76c9f-ff91-4431-b6dd-3b976c598020')

  -- Used to test artist_select
  --WHERE artist.id IN (1234440)

  -- Used to test area_select
  --WHERE artist.area IN (222)

  -- Used to test area_alias_select...
  --WHERE artist.area IN (2547)

  -- Used to test artist_alias
  -- also a great example of a result without an area, but has a language code
  --WHERE artist.id in (34491)

  -- Used to test artist_urls
  -- also good for Artist.begin_area
  --WHERE artist.id IN (4)

  -- Used to test artist.area_end
  --WHERE artist.id IN (1116675)

  -- Used to test artist_credit_select
  -- SELECT
  --   count(artist_credit_name.artist) as acount,
  --   artist_credit_name.artist_credit,
  --   artist_credit_name.artist
  -- FROM artist_credit_name
  -- GROUP BY artist_credit_name.artist, artist_credit
  -- ORDER BY acount DESC;
  -- artist_credit: 1123495, artist: 440635
  -- This is a reverse join, meaning we have to join from a right side. Yet to have the grammar of the 
  --  sql file line up, we'll have to do that with another CTE. Surprisingly, this is rather performant even
  --  with its complexity.
  -- To test it, you'll have to put a where 'artist.id' in here and a where 'artist_credit.id' in the artist_credit_select CTE
  -- WHERE artist.id in (440635)
), artist_alias_select AS (
  SELECT
    artist.id AS artist_id,
    ARRAY_AGG(
      json_object(
        ARRAY['name', 'id', 'locale', 'type', 'begin_year', 'end_year', 'data_type'],
        ARRAY[
          artist_alias.name::TEXT,
          artist_alias.id::TEXT,
          artist_alias.locale::TEXT,
          artist_alias_type.name::TEXT,
          artist_alias.begin_date_year::TEXT,
          artist_alias.end_date_year::TEXT,
          'artist_alias'
        ])) AS aliases
  FROM artist_alias
  INNER JOIN artist ON (artist_alias.artist = artist.id)
  INNER JOIN artist_alias_type ON (artist_alias.type = artist_alias_type.id)
  GROUP BY artist.id
), area_select AS (
  SELECT
    area.id AS area_id,
    area.gid AS guid,
    area.name AS name,
    area.begin_date_year AS begin_year,
    area.end_date_year AS end_year,
    ARRAY_AGG(
      json_object(
        ARRAY['id', 'name', 'sort_name', 'type', 'begin_year', 'end_year', 'data_type'],
        ARRAY[
          area_alias.id::TEXT,
          area_alias.name::TEXT,
          area_alias.sort_name::TEXT,
          area_alias_type.name::TEXT,
          area_alias.begin_date_year::TEXT,
          area_alias.end_date_year::TEXT,
          'area_alias'
        ])) as aliases,
    iso_3166_1.code AS iso_3166_1,
    iso_3166_2.code AS iso_3166_2,
    iso_3166_3.code AS iso_3166_3,
    'area' AS data_type
  FROM area
  LEFT JOIN area_alias ON (area.id = area_alias.area)
  INNER JOIN area_alias_type ON (area_alias.type = area_alias_type.id)
  LEFT JOIN iso_3166_1 ON (area.id = iso_3166_1.area)
  LEFT JOIN iso_3166_2 ON (area.id = iso_3166_2.area)
  LEFT JOIN iso_3166_3 ON (area.id = iso_3166_3.area)
  GROUP BY area.id, area.gid, area.name, area.begin_date_year, area.end_date_year, iso_3166_1.code, iso_3166_2.code, iso_3166_3.code, data_type
), artist_area_begin AS (
  SELECT
    artist.id AS artist_id,
    area_select AS begin_area
  FROM artist
  LEFT JOIN area_select ON (artist.begin_area = area_select.area_id)
), artist_area_end AS (
  SELECT
    artist.id AS artist_id,
    area_select AS end_area
  FROM artist
  LEFT JOIN area_select ON (artist.end_area = area_select.area_id)
), artist_area_select AS (
  SELECT
    artist.id AS artist_id,
    ARRAY_AGG(
      row_to_json(area_select)) AS areas
  FROM artist
  INNER JOIN area_select ON (artist.area = area_select.area_id)
  GROUP BY artist.id
), artist_tags AS (
  SELECT
    artist_tag.artist as artist_id,
    ARRAY_AGG(
      json_object(
        ARRAY['name', 'data_type'],
        ARRAY[tag.name, 'artist_tag'])) AS tags
    FROM artist_tag
    INNER JOIN tag ON (artist_tag.tag = tag.id)
    GROUP BY artist_tag.artist
), artist_urls AS (
  SELECT
    l_artist_url.entity0 AS artist_id,
    ARRAY_AGG(
      json_object(
        ARRAY['url', 'data_type'],
        ARRAY[url.url, 'artist_url'])) AS urls
  FROM l_artist_url
  INNER JOIN url ON (l_artist_url.entity1 = url.id)
  GROUP BY l_artist_url.entity0
--), aggregate_artist_credits AS (
), artist_credit_select AS (
  SELECT 
    ARRAY_AGG(artist_credit_name.name) AS artist_credits,
    artist AS artist_id,
    'artist_credits' AS data_type
  FROM artist_credit_name
  GROUP BY artist_id
), enumerate_artist_properties AS (
  SELECT
    artist_select.name AS name,
    artist_select.artist_id AS artist_id,
    artist_select.guid AS guid,
    artist_select.sort_name AS sort_name,
    artist_select.type AS type,
    artist_select.gender AS gender,
    artist_select.ipi AS ipi,
    artist_select.isni AS isni,
    artist_area_begin.begin_area AS begin_area,
    artist_area_end.end_area AS end_area,
    artist_alias_select.aliases AS aliases,
    artist_area_select.areas AS areas,
    artist_urls.urls AS urls,
    artist_tags.tags AS tags,
    artist_credit_select.artist_credits AS artist_credits,
    'artist' AS data_type
  FROM artist_select
  LEFT JOIN artist_alias_select ON (artist_select.artist_id = artist_alias_select.artist_id)
  LEFT JOIN artist_area_select ON (artist_select.artist_id = artist_area_select.artist_id)
  LEFT JOIN artist_urls ON (artist_select.artist_id = artist_urls.artist_id)
  LEFT JOIN artist_tags ON (artist_select.artist_id = artist_tags.artist_id)
  LEFT JOIN artist_area_begin ON (artist_select.artist_id = artist_area_begin.artist_id)
  LEFT JOIN artist_area_end ON (artist_select.artist_id = artist_area_end.artist_id)
  LEFT JOIN artist_credit_select ON (artist_select.artist_id = artist_credit_select.artist_id)
) SELECT row_to_json(row) FROM enumerate_artist_properties row;

