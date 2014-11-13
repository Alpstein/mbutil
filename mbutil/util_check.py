import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib, tempfile

from util import mbtiles_connect, execute_commands_on_tile, prettify_connect_string

logger = logging.getLogger(__name__)


def check_mbtiles(mbtiles_file, **kwargs):

    result = True

    scale       = kwargs.get('tile_scale', 1)
    zoom        = kwargs.get('zoom', -1)
    min_zoom    = kwargs.get('min_zoom', 0)
    max_zoom    = kwargs.get('max_zoom', 18)
    flip_tile_y = kwargs.get('flip_y', False)

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

    logger.info("Checking %s (%s)" % (prettify_connect_string(con.connect_string), zoom_level_string))


    logger.debug("Loading zoom levels")

    zoom_levels = con.zoom_levels(scale)
    missing_tiles = []

    for tile_z in zoom_levels:
        if tile_z < min_zoom or tile_z > max_zoom:
            continue

        logger.debug("Starting zoom level %d" % (tile_z))

        t = con.bounding_box_for_zoom_level(tile_z, scale)

        minX, maxX, minY, maxY = t[0], t[1], t[2], t[3]

        logger.debug(" - Checking zoom level %d, x: %d - %d, y: %d - %d" % (tile_z, minX, maxX, minY, maxY))

        for tile_y in range(minY, maxY+1):
            logger.debug("   - Row: %d (%.1f%%)" %
                (tile_y, (float(tile_y - minY) / float(maxY - minY)) * 100.0) if minY != maxY else 100.0)

            mbtiles_columns = con.columns_for_zoom_level_and_row(tile_z, tile_y, scale)

            for tile_x in range(minX, maxX+1):
                if tile_x not in mbtiles_columns:
                    if flip_tile_y:
                        tile_y = flip_y(tile_z, tile_y)
                    missing_tiles.append([tile_z, tile_x, tile_y])


    if len(missing_tiles) > 0:
        result = False
        logger.error("(zoom, x, y)")
        for current_tile in missing_tiles:
            logger.error(current_tile)


    con.close()

    if result:
        logger.info("Check succeeded")
    else:
        logger.info("Check failed")

    return result
