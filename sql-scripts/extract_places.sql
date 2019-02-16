WITH place_alias_sub_select AS (
  SELECT
    place_alias.id AS place_alias_id,
    place_alias.place AS place_id,
    place_alias.locale AS locale,
    place_alias.name AS name,
    place_alias_type.name AS type
  FROM place_alias
  LEFT JOIN place_alias_type ON (place_alias.type = place_alias_type.id)
), place_alias_select AS (
  SELECT
    place.id AS place_id,
    ARRAY_AGG(
      json_object(
        ARRAY['place_alias_id', 'locale', 'name', 'type', 'data_type'],
        ARRAY[
          place_alias_sub_select.place_alias_id::TEXT,
          place_alias_sub_select.locale::TEXT,
          place_alias_sub_select.name::TEXT,
          place_alias_sub_select.type::TEXT,
          'place_alias'::TEXT
        ])) AS aliases
  FROM place
  INNER JOIN place_alias_sub_select ON (place.id = place_alias_sub_select.place_id)
  GROUP BY place.id
), place_urls_select AS (
  SELECT
    l_place_url.entity0 AS place_id,
    ARRAY_AGG(
      json_object(
        ARRAY['url', 'data_type'],
        ARRAY[url.url, 'place_url'])) AS urls
  FROM l_place_url
  INNER JOIN url ON (l_place_url.entity1 = url.id)
  GROUP BY l_place_url.entity0
), place_tags_select AS (
  SELECT
    place_tag.place AS place_id,
    ARRAY_AGG(
      json_object(
        ARRAY['name', 'data_type'],
        ARRAY[tag.name, 'place_tag'])) AS tags
  FROM place_tag
  INNER JOIN tag ON (place_tag.tag = tag.id)
  GROUP BY place_tag.place
), place_select AS (
  SELECT
    place.id AS place_id,
    place.name AS name,
    place_type.name AS type,
    place.area AS area_id,
    place.address AS address,
    place_tags_select.tags AS tags,
    place_urls_select.urls AS urls,
    ARRAY[
      place.coordinates[0]::TEXT,
      place.coordinates[1]::TEXT] AS coordinates,
    place_alias_select.aliases AS aliases,
    'place'::TEXT AS data_type
  FROM place
  LEFT JOIN place_type ON (place.type = place_type.id)
  LEFT JOIN place_alias_select ON (place.id = place_alias_select.place_id)
  LEFT JOIN place_tags_select ON (place.id = place_tags_select.place_id)
  LEFT JOIN place_urls_select ON (place.id = place_urls_select.place_id)
  WHERE area IS NOT NULL
) SELECT row_to_json(row) from place_select row;

