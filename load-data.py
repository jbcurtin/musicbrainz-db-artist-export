#!/usr/bin/env python

'''
To run this script, please use pipenv, conda, or venv and install the following packages.

$ pip install pandas requests
'''
import logging
import os
import pandas
import requests
import lzma

logger = logging.getLogger('') # <--- Probable a good idea to name your logger. '' is the 'root' logger
sysHandler = logging.StreamHandler()
sysHandler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(sysHandler)
logger.setLevel(logging.INFO)

CHUNK_SIZE: int = 1024
ENCODING: str = 'utf-8'
OUTPUT_DIR: str = '/tmp/outputs'
if not os.path.exists(OUTPUT_DIR):
  s.makedirs(OUTPUT_DIR)

url: str = 'https://github.com/jbcurtin/musicbrainz-db-artist-export/blob/master/data/musicbrainz-db-artist-export.csv.xz?raw=true'
filename: str = os.path.basename(url.rsplit('?', 1)[0])
filepath: str = os.path.join(OUTPUT_DIR, filename)
with open(filepath, 'wb') as output_stream:
  logger.info(f'Downloading Data[{filename}]')
  response = requests.get(url, stream=True)
  for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
    output_stream.write(chunk)

filepath_csv: str = filepath.rsplit('.', 1)[0]
logger.info(f'Writing to file[{filepath_csv}]')
CHUNK_SIZE: int = 1024 * 4
with lzma.open(filepath, 'r') as compressed_stream:
  with open(filepath_csv, 'wb') as csv_stream:
    csv_stream.write(compressed_stream.read())

data_frame: pandas.core.frame.DataFrame = pandas.read_csv(filepath_csv, delimiter=',')
print(data_frame)

