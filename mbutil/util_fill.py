import sqlite3, uuid, sys, logging, time, os, re, json, zlib, hashlib, tempfile

from util import mbtiles_connect, coordinate_to_tile, prettify_connect_string, flip_y

logger = logging.getLogger(__name__)


def fill_mbtiles(mbtiles_file, image_filename, **kwargs):

    scale       = kwargs.get('tile_scale', 1)
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

    if tile_bbox != None and zoom < 0:
        logger.info("--tile-bbox can only be used with --zoom, exiting...")
        return

    if tile_bbox == None and bbox == None:
        logger.info("Either --tile-bbox or --bbox must be given, exiting...")
        return


    con = mbtiles_connect(mbtiles_file, auto_commit, journal_mode, synchronous_off, False)

    con.mbtiles_setup()

    if not con.is_compacted():
        con.close()
        logger.info("The mbtiles database must be compacted, exiting...")
        return


    zoom_level_string = None

    if min_zoom == max_zoom:
        zoom_level_string = "zoom level %d" % (min_zoom)
    else:
        zoom_level_string = "zoom levels %d -> %d" % (min_zoom, max_zoom)

    logger.info("Filling %s (%s)" % (prettify_connect_string(con.connect_string), zoom_level_string))


    # Insert an image
    tmp_file = open(image_filename, "r")
    tile_data = tmp_file.read()
    tmp_file.close()

    m = hashlib.md5()
    m.update(tile_data)
    tile_id = m.hexdigest()

    con.insert_tile_to_images(tile_id, tile_data)


    count = 0
    start_time = time.time()


    for tile_z in range(min_zoom, max_zoom+1):
        min_x = min_y = max_x = max_y = 0

        if tile_bbox != None:
            match = re.match(r'(\d+),(\d+),(\d+),(\d+)', tile_bbox, re.I)
            if match:
                min_x, min_y, max_x, max_y = int(match.group(1)), int(match.group(2)), int(match.group(3)), int(match.group(4))
        elif bbox != None:
            match = re.match(r'([-0-9\.]+),([-0-9\.]+),([-0-9\.]+),([-0-9\.]+)', bbox, re.I)
            if match:
                left, bottom, right, top = float(match.group(1)), float(match.group(2)), float(match.group(3)), float(match.group(4))
                min_x, min_y = coordinate_to_tile(left, bottom, tile_z)
                max_x, max_y = coordinate_to_tile(right, top, tile_z)

        if min_y > max_y:
            min_y, max_y = max_y, min_y

        for tile_x in range(min_x, max_x+1):
            for tile_y in range(min_y, max_y+1):
                if flip_tile_y:
                    tile_y = flip_y(tile_z, tile_y)

                # z, x, y
                con.insert_tile_to_map(tile_z, tile_x, tile_y, scale, tile_id, False) # Don't overwrite existing tiles

                count = count + 1
                if (count % 100) == 0:
                    logger.debug("%d tiles inserted (%.1f tiles/sec)" %
                        (count, count / (time.time() - start_time)))
                    if print_progress:
                        sys.stdout.write("\r%d tiles inserted (%.1f tiles/sec)" %
                            (count, count / (time.time() - start_time)))
                        sys.stdout.flush()


    if print_progress:
        sys.stdout.write('\n')

    logger.info("%d tiles inserted (100.0%% @ %.1f tiles/sec)" %
        (count, count / (time.time() - start_time)))
    if print_progress:
        sys.stdout.write("%d tiles inserted (100.0%% @ %.1f tiles/sec)\n" %
            (count, count / (time.time() - start_time)))
        sys.stdout.flush()


    con.optimize_database(kwargs.get('skip_analyze', False), kwargs.get('skip_vacuum', False))
    con.close()
