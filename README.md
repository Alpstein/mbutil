# MBUtil

MBUtil is a utility for importing and exporting the [MBTiles](http://mbtiles.org/) format,
typically created with [MapBox](http://mapbox.com/) [TileMill](http://mapbox.com/tilemill/).

Before exporting tiles to disk, see if there's a [MapBox Hosting plan](http://mapbox.com/plans/)
or an open source [MBTiles server implementation](https://github.com/mapbox/mbtiles-spec/wiki/Implementations)
that works for you - tiles on disk are notoriously difficult to manage.

## Installation

Git checkout (requires git)

    git clone git://github.com/Alpstein/mbutil.git
    cd mbutil
    ./mb-util -h

    # then to install the mb-util command globally:
    sudo python setup.py install
    # then you can run:
    mb-util

Python installation (requires easy_install)

    easy_install mbutil
    mb-util -h

## Usage

    Usage: mb-util [command] [options] file|directory [file|directory ...]

    Examples:

    Export an mbtiles database to a directory of files:
    $ mb-util --export world.mbtiles tiles

    Import a directory of tiles into an mbtiles database:
    $ mb-util --import tiles world.mbtiles

    Create an empty mbtiles file:
    $ mb-util --create empty.mbtiles

    Execute commands on all tiles in the mbtiles file:
    $ mb-util --process --execute "COMMAND ARGUMENTS" [--execute "SECOND COMMAND"] world.mbtiles

    Merge two or more mbtiles files (receiver will be the first file):
    $ mb-util --merge receiver.mbtiles file1.mbtiles [file2.mbtiles ...]

    Remove tiles older than 30 days from the database:
    $ mb-util --expire=30 world.mbtiles

    Fill a database with a given tile image
    $ mb-util --fill --min-zoom=7 --max-zoom=12 world.mbtiles transparent.png

    Check if a mbtiles file contains all tiles at a specific zoom level:
    $ mb-util --check --zoom=7 world.mbtiles

    Test tiles with a command, print tile coordinates for non-zero return values
    $ mb-util --test --execute "COMMAND ARGUMENTS" world.mbtiles

    Dumps a list of tiles to the console:
    $ mb-util --tilelist --as-bboxes --zoom=11 world.mbtiles

    Convert tile coordinates and bounding boxes:
    $ mb-util --convert="13/4328/2861"
    $ mb-util --convert="10.195312,47.546872,10.239258,47.576526" --min-zoom=12 --max-zoom=13

    Options:
        -h, --help            show this help message and exit

    Commands:
        These are the commands to use on mbtiles databases

        -e, --export        Export an mbtiles database to a directory of files. If
                            the directory exists, any already existing tiles will
                            be overwritten.
        -i, --import        Import a directory of tiles into an mbtiles database.
                            If the mbtiles database already exists, existing tiles
                            will be overwritten with the imported tiles.
        -m, --merge         Merge two or more databases. The receiver will be
                            created if it doesn't yet exist.
        -u, --update        Update one database from another, based on updated_at
                            timestamps. The receiver will be created if it doesn't
                            yet exist.
        -p, --process       Processes a mbtiles databases. Only usefull together
                            with one or more --execute.
        --expire=DAYS       Remove tiles older than DAYS from the database.
        --check             Check the database for missing tiles.
        --test              Test every tile with the given command, print the tile
                            coordinate if the command returns anything non-zero.
        --fill              Fill a database with tile images where it
                            doesn't already contain tiles. Only usefull with
                            --min-zoom/--max-zoom and --tile-bbox/--bbox.
        --create            Create an empty mbtiles database.
        --tilelist          Dumps a list of tiles to the console.
        --convert=CONVERT   Convert tile coordinates 'y/x/z' to bounding box
                            'left,bottom,right,top' or vice versa.

    Options:
        --execute=COMMAND   Commands to execute for each tile image. %s will be
                            replaced with the file name. This argument may be
                            repeated several times and can be used together with
                            --import/--export/--merge/--compact/--process.
        --flip-y            Flip the y tile coordinate during
                            --export/--import/--merge/--convert/--tilelist.
        --min-zoom=MIN_ZOOM
                            Minimum zoom level for --export/--import/--merge/--upd
                            ate/--process/--check/--convert.
        --max-zoom=MAX_ZOOM
                            Maximum zoom level for --export/--import/--merge/--upd
                            ate/--process/--check/--convert.
        --zoom=ZOOM         Zoom level for --export/--import/--merge/--update/--pr
                            ocess/--check/--convert. (Overrides --min-zoom and
                            --max-zoom)
        --min-timestamp=MIN_TIMESTAMP
                            Minimum numerical timestamp for
                            --export/--merge/--process/--test.
        --max-timestamp=MAX_TIMESTAMP
                            Maximum numerical timestamp for
                            --export/--merge/--process/--test.
        --bbox=BBOX         Bounding box in coordinates 'left,bottom,right,top'
                            (10.195312,47.546872,10.239258,47.576526)
        --tile-bbox=TILE_BBOX
                            Bounding box in tile coordinates
                            'left,bottom,right,top' (10,10,20,20). Can only be
                            used with --zoom.
        --revert-test       For --test, print the tile coordinates if the command
                            returns zero.
        --as-bboxes         For --tilelist, print the bounding boxes for tiles.
        --auto-commit       Enable auto commit.
        --synchronous-off   DANGEROUS!!! Set synchronous=OFF for SQLite database
                            connections.
        --use-wal-journal   Use journal_mode=WAL for SQLite databases.
                            [DEPRECATED]
        --journal_mode=JOURNAL_MODE
                            SQLite journal mode to use
                            (wal|delete|memory|truncate|persist|off). Defaults to
                            'wal'.
        --check-before-merge
                            Runs some basic checks (like --check) on databases
                            before merging them.
        --delete-after-export
                            DANGEROUS!!! After a --merge or --export, this option
                            will delete all the merged/exported tiles from the
                            (sending) database. Only really usefull with --min-
                            zoom/--max-zoom or --zoom since it would remove all
                            tiles from the database otherwise.
        --delete-vanished-tiles
                            DANGEROUS!!! If a tile vanishes during --execute then
                            delete it also from the database or ignore it during
                            --merge/--process.
        --poolsize=POOLSIZE
                            Pool size for processing tiles with --process/--merge.
                            Default is to use a pool size equal to the number of
                            cpu cores.
        --tmp-dir=TMP_DIR   Temporary directory to use for --execute (e.g.
                            /dev/shm).
        --vacuum            VACUUM the database after
                            --import/--merge/--process/--fill/--expire.
        --analyze           ANALYZE the database after
                            --import/--merge/--process/--fill/--expire.
        --progress          Print progress updates and keep them on one line.
        -q, --quiet         don't print any status messages to stdout except
                            errors.
        -d, --debug         print debug messages to stdout (exclusive to --quiet).


## Special considerations

* All mbtiles databases must be on the same host as the mb-util binary if you want to use the WAL locking mode.
* Using --synchronous-off is dangerous since your database might get corrupted.
* Use --tmp-dir=/dev/shm on Ubuntu to place temporary files on a ram disk.
* Use --use-wal-journal if you want to udpate a database which is at the same time used for reading.
* --auto-commit will disable transactions and therefore most probably slow down any insert operations to the database.
* --auto-commit is always enabled for Postgres databases.

## Requirements

* Python `>= 2.6`
* SQLite `>= 3.7.0`
* psycopg2 `>= 2.4.6`
* oursql `>= 0.9.3.1`
* PyMongo `>= 2.7.2`

## Metadata

mb-util imports and exports metadata as JSON, in the root of the tile directory, as a file named `metadata.json`.

```javascript
{
    "name": "World Light",
    "description": "A Test Metadata",
    "version": "3"
}
```

## Testing

This project uses [nosetests](http://readthedocs.org/docs/nose/en/latest/) for testing. Install nosetests
and run

    nosetests

## License

BSD - see LICENSE.md

## Authors

- Tom MacWright (tmcw)
- Dane Springmeyer (springmeyer)
- Mathieu Leplatre (leplatrem)
- Thomas Rasch (trasch)
