import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib

from util import mbtiles_connect, flip_y, prettify_connect_string

logger = logging.getLogger(__name__)


def update_mbtiles(mbtiles_file1, mbtiles_file2, **kwargs):

    scale    = kwargs.get('tile_scale', None)
    zoom     = kwargs.get('zoom', -1)
    min_zoom = kwargs.get('min_zoom', 0)
    max_zoom = kwargs.get('max_zoom', 18)

    auto_commit     = kwargs.get('auto_commit', False)
    journal_mode    = kwargs.get('journal_mode', 'wal')
    synchronous_off = kwargs.get('synchronous_off', False)

    print_progress  = kwargs.get('progress', False)
    flip_tile_y     = kwargs.get('flip_y', False)


    con1 = mbtiles_connect(mbtiles_file1, auto_commit, journal_mode, synchronous_off, False, False)
    con2 = mbtiles_connect(mbtiles_file2, auto_commit, journal_mode, synchronous_off, False, True)

    con1.mbtiles_setup()

    if not con1.is_compacted() or not con2.is_compacted:
        con1.close()
        con2.close()
        sys.stderr.write('To update mbtiles databases, both databases must already be compacted\n')
        sys.exit(1)


    zoom_level_string = None

    if min_zoom == max_zoom:
        zoom_level_string = "zoom level %d" % (min_zoom)
    else:
        zoom_level_string = "zoom levels %d -> %d" % (min_zoom, max_zoom)

    logger.info("Updating %s --> %s (%s)" % (prettify_connect_string(con2.connect_string), prettify_connect_string(con1.connect_string), zoom_level_string))


    # Check that the old and new image formats are the same
    original_format = new_format = None
    try:
        original_format = con1.metadata().get('format')
    except:
        pass

    try:
        new_format = con2.metadata().get('format')
    except:
        pass

    if new_format == None:
        logger.info("No image format found in the sending database, assuming 'png'")
        new_format = "png"

    if original_format != None and new_format != original_format:
        con1.close()
        con2.close()
        sys.stderr.write('The files to merge must use the same image format (png or jpg)\n')
        sys.exit(1)

    if original_format == None and new_format != None:
        con1.update_metadata("format", new_format)

    if new_format == None:
        new_format = original_format


    count = 0
    start_time = time.time()

    min_timestamp = con1.max_timestamp()
    if min_timestamp is None: min_timestamp = 0
    max_timestamp = int(time.time())

    total_tiles = con2.updates_count(min_zoom, max_zoom, min_timestamp, max_timestamp)

    if total_tiles == 0:
        con1.close()
        con2.close()
        sys.stderr.write('No tiles to update, exiting...\n')
        return

    logger.debug("%d tiles to update" % (total_tiles))
    if print_progress:
        sys.stdout.write("%d tiles to update\n" % (total_tiles))
        sys.stdout.write("0 tiles updated (0% @ 0 tiles/sec)")
        sys.stdout.flush()


    known_tile_ids = set()
    tmp_images_list = []
    tmp_row_list = []

    deleted_tiles_count = 0

    for t in con2.updates(min_zoom, max_zoom, min_timestamp, max_timestamp):
        tile_z = t[0]
        tile_x = t[1]
        tile_y = t[2]
        tile_scale = t[3]
        tile_data = str(t[4])
        tile_id = t[5]

        if flip_tile_y:
            tile_y = flip_y(tile_z, tile_y)

        if tile_id is None:
            deleted_tiles_count += 1

        if tile_id and tile_id not in known_tile_ids:
            tmp_images_list.append( (tile_id, tile_data) )
            known_tile_ids.add(tile_id)

        tmp_row_list.append( (tile_z, tile_x, tile_y, tile_scale, tile_id, int(time.time())) )

        count = count + 1
        if (count % 100) == 0:
            logger.debug("%d tiles merged (%.1f%% @ %.1f tiles/sec)" % (count, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))
            if print_progress:
                sys.stdout.write("\r%d tiles merged (%.1f%% @ %.1f tiles/sec)" % (count, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))
                sys.stdout.flush()

        if len(tmp_images_list) > 250:
            con1.insert_tiles_to_images(tmp_images_list)
            tmp_images_list = []

        if len(tmp_row_list) > 250:
            con1.insert_tiles_to_map(tmp_row_list)
            tmp_row_list = []

    # Push the remaining rows to the database
    if len(tmp_images_list) > 0:
        con1.insert_tiles_to_images(tmp_images_list)

    if len(tmp_row_list) > 0:
        con1.insert_tiles_to_map(tmp_row_list)


    if print_progress:
        sys.stdout.write('\n')

    logger.info("%d tiles merged (100.0%% @ %.1f tiles/sec)" % (count, count / (time.time() - start_time)))
    if print_progress:
        sys.stdout.write("%d tiles merged (100.0%% @ %.1f tiles/sec)\n" % (count, count / (time.time() - start_time)))
        sys.stdout.flush()


    if deleted_tiles_count > 0:
        logger.info("Deleting %d orphaned image(s)" % (deleted_tiles_count))
        if print_progress:
            sys.stdout.write("Deleting %d orphaned image(s)\n" % (deleted_tiles_count))
            sys.stdout.flush()
        con1.delete_orphaned_images()


    con1.close()
    con2.close()
