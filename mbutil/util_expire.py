import sys, logging, sqlite3, time

logger = logging.getLogger(__name__)

from util import mbtiles_connect, prettify_connect_string
from util_convert import parse_and_convert_tile_bbox, parse_bbox, tiles_for_bbox

def expire_mbtiles(mbtiles_file, **kwargs):

    scale       = kwargs.get('tile_scale', None)
    zoom        = kwargs.get('zoom', -1)
    min_zoom    = kwargs.get('min_zoom', 0)
    max_zoom    = kwargs.get('max_zoom', 18)

    auto_commit     = kwargs.get('auto_commit', False)
    journal_mode    = kwargs.get('journal_mode', 'wal')
    synchronous_off = kwargs.get('synchronous_off', False)
    expire_days     = kwargs.get('expire', 0)

    if expire_days == 0:
        return

    if zoom >= 0:
        min_zoom = max_zoom = zoom
    elif min_zoom == max_zoom:
        zoom = min_zoom


    con = mbtiles_connect(mbtiles_file, auto_commit, journal_mode, synchronous_off, False, True)

    logger.info("Expiring tiles from %s" % (prettify_connect_string(con.connect_string)))

    expire_timestamp = (int(time.time()) - (int(expire_days) * 86400))

    logger.debug("Expiring tiles older than %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(expire_timestamp))))

    con.expire_tiles(min_zoom, max_zoom, 0, expire_timestamp, scale)

    con.optimize_database(kwargs.get('skip_analyze', False), kwargs.get('skip_vacuum', False))
    con.close()


def expire_tiles_bbox(mbtiles_file, **kwargs):

    scale       = kwargs.get('tile_scale', None)
    zoom        = kwargs.get('zoom', -1)
    min_zoom    = kwargs.get('min_zoom', 0)
    max_zoom    = kwargs.get('max_zoom', 18)

    flip_tile_y = kwargs.get('flip_y', False)
    bbox        = kwargs.get('bbox', None)
    tile_bbox   = kwargs.get('tile_bbox', None)

    auto_commit     = kwargs.get('auto_commit', False)
    journal_mode    = kwargs.get('journal_mode', 'wal')
    synchronous_off = kwargs.get('synchronous_off', False)

    print_progress  = kwargs.get('progress', False)

    if zoom >= 0:
        min_zoom = max_zoom = zoom
    elif min_zoom == max_zoom:
        zoom = min_zoom


    if tile_bbox == None and bbox == None:
        logger.info("Either --tile-bbox or --bbox must be given, exiting...")
        return


    min_x = min_y = max_x = max_y = 0

    if tile_bbox:
        min_x, min_y, max_x, max_y = parse_and_convert_tile_bbox(tile_bbox, flip_tile_y)
    else:
        min_x, min_y, max_x, max_y = parse_bbox(bbox)


    con = mbtiles_connect(mbtiles_file, auto_commit, journal_mode, synchronous_off, False, True)

    logger.info("Expiring tiles from %s" % (prettify_connect_string(con.connect_string)))

    for tile_z in range(min_zoom, max_zoom+1):
        for tile_z, tile_x, tile_y in tiles_for_bbox(min_x, min_y, max_x, max_y, tile_z, flip_tile_y):
            logger.debug("Expiring tile %d/%d/%d" % (tile_z, tile_x, tile_y))
            if print_progress:
                sys.stdout.write("\rExpiring tile %d/%d/%d" % (tile_z, tile_x, tile_y))

            con.expire_tile(tile_z, tile_x, tile_y, scale)


    if print_progress:
        sys.stdout.write('\n')

    con.optimize_database(kwargs.get('skip_analyze', False), kwargs.get('skip_vacuum', False))
    con.close()
