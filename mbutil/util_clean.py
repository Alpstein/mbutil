import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib, tempfile

from util import mbtiles_connect, prettify_connect_string

logger = logging.getLogger(__name__)


def clean_mbtiles(mbtiles_file, **kwargs):

    result = True

    auto_commit     = kwargs.get('auto_commit', False)
    journal_mode    = kwargs.get('journal_mode', 'wal')
    synchronous_off = kwargs.get('synchronous_off', False)


    con = mbtiles_connect(mbtiles_file, auto_commit, journal_mode, synchronous_off, False, True)


    logger.info("Cleaning %s" % (prettify_connect_string(con.connect_string)))

    con.delete_orphaned_images()

    con.close()

    return result
