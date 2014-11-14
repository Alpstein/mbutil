import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib, tempfile

from util import mbtiles_connect, prettify_connect_string, flip_y
from util_convert import convert_tile_to_bbox

logger = logging.getLogger(__name__)


def mbtiles_tilelist(mbtiles_file, **kwargs):

    flip_tile_y = kwargs.get('flip_y', False)
    as_bboxes   = kwargs.get('as_bboxes', False)

    scale     = kwargs.get('tile_scale', None)
    zoom      = kwargs.get('zoom', -1)
    min_zoom  = kwargs.get('min_zoom', 0)
    max_zoom  = kwargs.get('max_zoom', 18)

    auto_commit     = kwargs.get('auto_commit', False)
    journal_mode    = kwargs.get('journal_mode', 'wal')
    synchronous_off = kwargs.get('synchronous_off', False)

    if zoom >= 0:
        min_zoom = max_zoom = zoom


    con = mbtiles_connect(mbtiles_file, auto_commit, journal_mode, synchronous_off, False, True)


    zoom_level_string = None

    if min_zoom == max_zoom:
        zoom_level_string = "zoom level %d" % (min_zoom)
    else:
        zoom_level_string = "zoom levels %d -> %d" % (min_zoom, max_zoom)

    logger.info("Tile list for %s (%s)" % (prettify_connect_string(con.connect_string), zoom_level_string))


    for tile_z in range(min_zoom, max_zoom+1):
        logger.debug("Starting zoom level %d" % (tile_z))

        for t in con.columns_and_rows_for_zoom_level(tile_z, scale):
            tile_x, tile_y = int(t[0]), int(t[1])

            if as_bboxes:
                convert_tile_to_bbox(tile_z, tile_x, tile_y, flip_tile_y)
            else:
                if flip_tile_y:
                    tile_y = flip_y(tile_z, tile_y)
                sys.stdout.write("%d/%d/%d\n" % (tile_z, tile_x, tile_y))


    con.close()
