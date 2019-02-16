#!/usr/bin/env python

import aiofiles
import asyncio
import csv
import gzip
import hashlib
import io
import json
import logging
import lzma
import os
import ssl
import typing

from urllib.parse import ParseResult, urlparse

logger = logging.getLogger('') # <--- Probable a good idea to name your logger. '' is the 'root' logger
sysHandler = logging.StreamHandler()
sysHandler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(sysHandler)
logger.setLevel(logging.INFO)

import uvloop
import asyncpg

REDIS_HOST: str = 'localhost'
REDIS_PORT: int = 6379
REDIS_DB: int = 5

REDIS_POOL: typing.Any = None
DB_POOL: typing.Any = None
ENCODING: str = 'utf-8'
PREFTECH_COUNT: int = 10000
OUTPUT_DIR: str = os.path.join(os.getcwd(), 'data')
ARTIST_OUTPUT_PATH: str = os.path.join(OUTPUT_DIR, 'musicbrainz-db-artist-export.csv.xz')
PLACES_OUTPUT_PATH: str = os.path.join(OUTPUT_DIR, 'musicbrainz-db-places-export.csv.xz')

SQL_TOOLS_PATH: str = os.path.join(os.getcwd(), 'sql-scripts')
ARTIST_SQL_PATH: str = os.path.join(SQL_TOOLS_PATH, 'extract_artists.sql')
PLACES_SQL_PATH: str = os.path.join(SQL_TOOLS_PATH, 'extract_places.sql')

if not os.path.exists(OUTPUT_DIR):
  os.makedirs(OUTPUT_DIR)

if os.path.exists(ARTIST_OUTPUT_PATH):
  os.remove(ARTIST_OUTPUT_PATH)

if os.path.exists(PLACES_OUTPUT_PATH):
  os.remove(PLACES_OUTPUT_PATH)

async def setup() -> None:
  logger.info('Running Setup')
  uri: str = os.environ['PSQL_URI']
  url_parts: ParseResult = urlparse(os.environ['PSQL_URI'])
  os.environ['PGUSER']: str = url_parts.netloc.split(':', 1)[0]
  os.environ['PGPASSWORD']: str = url_parts.netloc.split('@', 1)[0].rsplit(':', 1)[1]
  os.environ['PGHOST']: str = url_parts.netloc.split('@', 1)[1].split(':', 1)[0]
  os.environ['PGPORT']: str = url_parts.netloc.split('@', 1)[1].rsplit(':', 1)[1]
  os.environ['PGDATABASE']: str = url_parts.path.strip('/')

  global DB_POOL
  ctx: ssl.SSLContext = ssl.SSLContext()
  DB_POOL = await asyncpg.create_pool(ssl=ctx)

async def _commit_results(results: typing.List[typing.Any], headers: typing.List[str] = None, write_headers: bool = False, filepath: str = None) -> None:
  logger.info(f'Writing Records[{len(results)}] to File[{filepath}]')
  memory_stream: io.StringIO = io.StringIO()
  writer: csv.Writer = csv.writer(memory_stream, delimiter=',', lineterminator='\n', quoting=csv.QUOTE_MINIMAL)

  if write_headers:
    writer.writerow(headers)

  for entry in results:
    writer.writerow(entry)

  memory_stream.seek(0)
  with lzma.open(filepath, 'ab') as stream:
    stream.write(memory_stream.read().encode(ENCODING))

def _hash_datum(datum: typing.List[typing.Any]) -> str:
  for idx, item in enumerate(datum):
    if item is None:
      datum[idx] = []

    elif not item.__class__ == list:
      datum[idx] = [item]

  sorted_datum: str = ''.join(''.join(sorted([item for sublist in datum for item in sublist])).split(' '))
  return hashlib.md5(sorted_datum.encode(ENCODING)).hexdigest()

async def extract_places() -> None:
  async with aiofiles.open(PLACES_SQL_PATH, 'rb') as stream:
    places_sql: str = (await stream.read()).decode(ENCODING)

  first_write: bool = True
  headers: typing.List[str] = ['name', 'coordinates', 'aliases', 'urls', 'tags']
  results: typing.List[typing.Dict[str, typing.Any]] = []
  datum_hashes: typing.List[str] = []
  async with DB_POOL.acquire() as conn:
    async with conn.transaction():
      async for entry in conn.cursor(places_sql, prefetch=PREFTECH_COUNT):
        item: typing.Dict[str, typing.Any] = json.loads(entry.get('row_to_json'))
        if item['tags'] is None:
          item['tags'] = []

        if item['urls'] is None:
          item['urls'] = []

        if item['coordinates'] is None:
          item['coordinates'] = []
        elif item['coordinates'][0] == None or item['coordinates'][1] == None:
          item['coordinates'] = []

        if item['aliases'] is None:
          item['aliases'] = []

        aliases: typing.List[str] = [part['name'] for part in item['aliases']]
        urls: typing.List[str] = [part['url'] for part in item['urls']]
        tags: typing.List[str] = [part['name'] for part in item['tags']]

        datum: typing.List[str] = [item['name'], item['coordinates'], aliases, urls, tags]
        datum_hash: str = _hash_datum(datum)
        if datum_hash in datum_hashes:
          continue

        datum_hashes.append(datum_hash)
        results.append(datum)
        if len(results) > PREFTECH_COUNT * 5:
          if first_write:
            await _commit_results(results, headers, True, PLACES_OUTPUT_PATH)
            first_write: bool = False 
          else:
            await _commit_results(results, headers, False, PLACES_OUTPUT_PATH)

          results = []

  if len(results) > 0:
    await _commit_results(results, headers, False, PLACES_OUTPUT_PATH)

async def extract_artists() -> None:
  async with aiofiles.open(ARTIST_SQL_PATH, 'rb') as stream:
    artist_sql: str = (await stream.read()).decode(ENCODING)

  first_write: bool = True
  headers: typing.List[typing.Any] = ['name', 'isni', 'ipi', 'musicbrainz_guid', 'aliases', 'tags', 'artist_credits', 'urls', 'areas']
  results: typing.List[typing.Dict[str, typing.Any]] = []
  datum_hashes: typing.List[str] = []
  async with DB_POOL.acquire() as conn:
    async with conn.transaction():
      async for entry in conn.cursor(artist_sql, prefetch=PREFTECH_COUNT):
        item: typing.Dict[str, typing.Any] = json.loads(entry.get('row_to_json'))
        if item['aliases'] is None:
          item['aliases'] = []

        if item['tags'] is None:
          item['tags'] = []

        if item['urls'] is None:
          item['urls' ] = []

        if item['artist_credits'] is None:
          item['artist_credits'] = []

        if item['areas'] is None:
          item['areas'] = []

        areas: typing.List[str] = []
        for area in item['areas']:
          areas.append(area['name'])
          areas.extend({alias['name'] for alias in area['aliases']})

        if item['begin_area']:
          areas.append(item['begin_area']['name'])
          areas.extend({alias['name'] for alias in item['begin_area']['aliases']})

        if item['end_area']:
          areas.append(item['end_area']['name'])
          areas.extend({alias['name'] for alias in item['end_area']['aliases']})

        name: str = item['name']
        isni: str = item.get('isni', None)
        ipi: str = item.get('ipi', None)
        musicbrainz_guid: str = item['guid']
        aliases: typing.List[str] = list({ali['name'] for ali in item.get('aliases', [])})
        tags: typing.List[str] = list({tag['name'] for tag in item.get('tags', [])})
        artist_credits: typing.List[str] = list({ac_name for ac_name in item.get('artist_credits', [])})
        urls: typing.List[str] = list({url['url'] for url in item.get('urls', [])})
        datum: typing.List[typing.Any] = [name, isni, ipi, musicbrainz_guid, aliases, tags, artist_credits, urls, areas]
        datum_hash: str = _hash_datum(datum)
        if datum_hash in datum_hashes:
          continue

        datum_hashes.append(datum_hash)
        results.append(datum)
        if len(results) > PREFTECH_COUNT * 5:
          if first_write:
            await _commit_results(results, headers, True, ARTIST_OUTPUT_PATH)
            first_write: bool = False
          else:
            await _commit_results(results, headers, False, ARTIST_OUTPUT_PATH)

          results = []

  if len(results) > 0:
    await _commit_results(results, headers, False, ARTIST_OUTPUT_PATH)

async def main() -> None:
  await setup()
  await extract_artists()
  await extract_places()

def capture_options() -> typing.Any:
  return {}

if __name__ in ['__main__']:
  options = capture_options()
  EVENT_LOOP = asyncio.get_event_loop()
  EVENT_LOOP.run_until_complete(main())


