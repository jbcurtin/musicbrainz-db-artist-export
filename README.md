# Musicbrainz DB Export Artists

## About musicbrainz-db-artist-export
This repository contains [artists and places](https://github.com/jbcurtin/musicbrainz-db-artist-export/tree/master/sql-scripts) from Musicbrainz DB

### Loading Data

```
$ git clone https://github.com/jbcurtin/musicbrainz-db-artist-export.git $HOME/musicbrainz-db-artist-export
$ cd $HOME/musicbrainz-db-artist-export
$ virtualenv -p $(which python3) env
$ source env/bin/activate
$ pip install pandas requests
$ python load-data.py
```

## About Musicbrainz DB
MusicBrainz is a community-maintained open source encyclopedia of music information. Musicbrainz DB Export Artists is intended to provide access the broader audiance of Data Scientist modern toolchains. The majority of the data in the MusicBrainz Database is licensed under the CC0, which is effectively placing the data into the Public Domain. That means that anyone can download the data and use it in any way they see fit. The remaining data is released under a Creative Commons Attribution-NonCommercial-ShareAlike 3.0 license.

[For more information, please read about Musicbrainz.org](https://musicbrainz.org/doc/About)

### Musicbrainz DB Schema
https://musicbrainz.org/doc/MusicBrainz_Database/Schema#Schema


## LICENSE
Only codes and descriptions in repo are LICENSE'd under MIT. Please read up on Musicbrainz License for everything else
