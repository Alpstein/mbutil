import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib, tempfile

from util import mbtiles_connect, execute_commands_on_tile, flip_y, prettify_connect_string

logger = logging.getLogger(__name__)


def mbtiles_to_disk(mbtiles_file, directory_path, **kwargs):

    delete_after_export = kwargs.get('delete_after_export', False)

    auto_commit     = kwargs.get('auto_commit', False)
    journal_mode    = kwargs.get('journal_mode', 'wal')
    synchronous_off = kwargs.get('synchronous_off', False)

    scale    = kwargs.get('tile_scale', None)
    zoom     = kwargs.get('zoom', -1)
    min_zoom = kwargs.get('min_zoom', 0)
    max_zoom = kwargs.get('max_zoom', 18)
    tmp_dir  = kwargs.get('tmp_dir', None)

    print_progress = kwargs.get('progress', False)
    flip_tile_y    = kwargs.get('flip_y', False)
    min_timestamp  = kwargs.get('min_timestamp', 0)
    max_timestamp  = kwargs.get('max_timestamp', 0)

    if tmp_dir and not os.path.isdir(tmp_dir):
        os.mkdir(tmp_dir)

    if zoom >= 0:
        min_zoom = max_zoom = zoom


    con = mbtiles_connect(mbtiles_file, auto_commit, journal_mode, synchronous_off, False, True)


    zoom_level_string = None

    if min_zoom == max_zoom:
        zoom_level_string = "zoom level %d" % (min_zoom)
    else:
        zoom_level_string = "zoom levels %d -> %d" % (min_zoom, max_zoom)

    logger.info("Exporting %s --> path:'%s' (%s)" % (prettify_connect_string(con.connect_string), directory_path, zoom_level_string))


    if not os.path.isdir(directory_path):
        os.mkdir(directory_path)
    base_path = os.path.join(directory_path, "tiles")
    if not os.path.isdir(base_path):
        os.makedirs(base_path)


    metadata = con.metadata()
    json.dump(metadata, open(os.path.join(directory_path, 'metadata.json'), 'w'), indent=4)

    count = 0
    start_time = time.time()
    image_format = metadata.get('format', 'png')
    sending_mbtiles_is_compacted = con.is_compacted()

    if not sending_mbtiles_is_compacted and (min_timestamp != 0 or max_timestamp != 0):
        con.close()
        sys.stderr.write('min-timestamp/max-timestamp can only be used with compacted databases.\n')
        sys.exit(1)


    total_tiles = con.tiles_count(min_zoom, max_zoom, min_timestamp, max_timestamp, scale)

    logger.debug("%d tiles to export" % (total_tiles))
    if print_progress:
        sys.stdout.write("%d tiles to export\n" % (total_tiles))
        sys.stdout.write("%d / %d tiles exported (0%% @ 0 tiles/sec)" % (count, total_tiles))
        sys.stdout.flush()


    for t in con.tiles(min_zoom, max_zoom, min_timestamp, max_timestamp, scale):
        tile_z = t[0]
        tile_x = t[1]
        tile_y = t[2]
        tile_scale = t[3]
        tile_data = str(t[4])

        # Execute commands
        if kwargs.get('command_list'):
            tile_data = execute_commands_on_tile(kwargs['command_list'], image_format, tile_data, tmp_dir)

        if flip_tile_y:
            tile_y = flip_y(tile_z, tile_y)

        tile_dir = os.path.join(base_path, str(tile_z), str(tile_x))
        if not os.path.isdir(tile_dir):
            os.makedirs(tile_dir)

        tile_file = os.path.join(tile_dir, '%s.%s' % (tile_y, metadata.get('format', 'png')))

        f = open(tile_file, 'wb')
        f.write(tile_data)
        f.close()

        count = count + 1
        if (count % 100) == 0:
            logger.debug("%d / %d tiles exported (%.1f%% @ %.1f tiles/sec)" %
                (count, total_tiles, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))
            if print_progress:
                sys.stdout.write("\r%d / %d tiles exported (%.1f%% @ %.1f tiles/sec)" %
                    (count, total_tiles, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))
                sys.stdout.flush()


    if print_progress:
        sys.stdout.write('\n')

    logger.info("%d / %d tiles exported (100.0%% @ %.1f tiles/sec)" % (count, total_tiles, count / (time.time() - start_time)))
    if print_progress:
        sys.stdout.write("%d / %d tiles exported (100.0%% @ %.1f tiles/sec)\n" % (count, total_tiles, count / (time.time() - start_time)))
        sys.stdout.flush()


    if delete_after_export:
        logger.debug("WARNING: Removing exported tiles from %s" % (mbtiles_file))

        con.delete_tiles(min_zoom, max_zoom, min_timestamp, max_timestamp, scale)
        con.optimize_database(kwargs.get('skip_analyze', False), kwargs.get('skip_vacuum', False))


    con.close()

