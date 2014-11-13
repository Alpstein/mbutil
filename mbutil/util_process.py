import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib, tempfile, multiprocessing

from util import mbtiles_connect, process_tile, prettify_connect_string
from multiprocessing import Pool

logger = logging.getLogger(__name__)


def process_tiles(pool, tiles_to_process, con, count, total_tiles, start_time, print_progress, delete_vanished_tiles):
    # Execute commands in parallel
    # logger.debug("Starting multiprocessing...")
    processed_tiles = pool.map(process_tile, tiles_to_process)

    # logger.debug("Starting reimport...")
    for next_tile in processed_tiles:
        tile_data = None
        tile_id, tile_file_path, original_size = next_tile['tile_id'], next_tile['filename'], next_tile['size']

        if os.path.isfile(tile_file_path):
            tmp_file = open(tile_file_path, "r")
            tile_data = tmp_file.read()
            tmp_file.close()

            os.remove(tile_file_path)

            if tile_data and len(tile_data) > 0:
                m = hashlib.md5()
                m.update(tile_data)
                new_tile_id = m.hexdigest()

                con.update_tile(tile_id, new_tile_id, tile_data)

                # logger.debug("Tile %s done\n" % (tile_id, ))
        else:
            if delete_vanished_tiles:
                con.delete_tile_with_id(tile_id)
                logger.debug("Removed vanished tile %s" % (tile_id, ))


        count = count + 1
        if (count % 100) == 0:
            logger.debug("%d tiles finished (%.1f%% @ %.1f tiles/sec)" %
                (count, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))
            if print_progress:
                sys.stdout.write("\r%d tiles finished (%.1f%% @ %.1f tiles/sec)" %
                    (count, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))
                sys.stdout.flush()

    return count


def execute_commands_on_mbtiles(mbtiles_file, **kwargs):

    if kwargs.get('command_list') == None or len(kwargs['command_list']) == 0:
        return

    auto_commit     = kwargs.get('auto_commit', False)
    journal_mode    = kwargs.get('journal_mode', 'wal')
    synchronous_off = kwargs.get('synchronous_off', False)

    scale        = kwargs.get('tile_scale', 1)
    zoom         = kwargs.get('zoom', -1)
    min_zoom     = kwargs.get('min_zoom', 0)
    max_zoom     = kwargs.get('max_zoom', 18)
    tmp_dir      = kwargs.get('tmp_dir', None)

    default_pool_size = kwargs.get('poolsize', -1)
    print_progress    = kwargs.get('progress', False)
    min_timestamp     = kwargs.get('min_timestamp', 0)
    max_timestamp     = kwargs.get('max_timestamp', 0)

    delete_vanished_tiles = kwargs.get('delete_vanished_tiles', False)

    if tmp_dir and not os.path.isdir(tmp_dir):
        os.mkdir(tmp_dir)

    if zoom >= 0:
        min_zoom = max_zoom = zoom


    con = mbtiles_connect(mbtiles_file, auto_commit, journal_mode, synchronous_off, False, True)

    if not con.is_compacted():
        con.close()
        logger.info("The mbtiles database must be compacted, exiting...")
        return

    con.mbtiles_setup()

    zoom_level_string = None

    if min_zoom == max_zoom:
        zoom_level_string = "zoom level %d" % (min_zoom)
    else:
        zoom_level_string = "zoom levels %d -> %d" % (min_zoom, max_zoom)

    logger.info("Executing commands on %s (%s)" % (prettify_connect_string(con.connect_string), zoom_level_string))


    image_format = 'png'

    metadata = con.metadata()
    if metadata.has_key('format'):
        image_format = metadata['format']


    count = 0
    duplicates = 0
    chunk = 1000
    start_time = time.time()

    total_tiles = con.tiles_count(min_zoom, max_zoom, min_timestamp, max_timestamp, scale)

    logger.debug("%d tiles to process" % (total_tiles))
    if print_progress:
        sys.stdout.write("%d tiles to process\n" % (total_tiles))
        sys.stdout.write("0 tiles finished (0% @ 0 tiles/sec)")
        sys.stdout.flush()


    logger.debug("Creating an index for the tile_id column...")
    con.create_map_tile_index()
    logger.debug("...done")


    if default_pool_size < 1:
        default_pool_size = None
        logger.debug("Using default pool size")
    else:
        logger.debug("Using pool size = %d" % (default_pool_size))

    pool = Pool(default_pool_size)
    multiprocessing.log_to_stderr(logger.level)


    tiles_to_process = []
    processed_tile_ids = set()

    for t in con.tiles_with_tile_id(min_zoom, max_zoom, min_timestamp, max_timestamp, scale):
        tile_z = t[0]
        tile_x = t[1]
        tile_y = t[2]
        tile_scale = t[3]
        tile_data = str(t[4])
        tile_id = t[5]
        # logging.debug("Working on tile (%d, %d, %d)" % (tile_z, tile_x, tile_y))

        if tile_id in processed_tile_ids:
            duplicates = duplicates + 1
        else:
            processed_tile_ids.add(tile_id)

        tmp_file_fd, tmp_file_name = tempfile.mkstemp(suffix=".%s" % (image_format), prefix="tile_", dir=tmp_dir)
        tmp_file = os.fdopen(tmp_file_fd, "w")
        tmp_file.write(tile_data)
        tmp_file.close()

        tiles_to_process.append({
            'tile_id' : tile_id,
            'tile_x' : tile_x,
            'tile_y' : tile_y,
            'tile_z' : tile_z,
            'filename' : tmp_file_name,
            'format' : image_format,
            'size' : len(tile_data),
            'command_list' : kwargs.get('command_list', [])
        })

        if len(tiles_to_process) < chunk:
            continue

        count = process_tiles(pool, tiles_to_process, con, count, total_tiles, start_time, print_progress, delete_vanished_tiles)

        tiles_to_process = []


    if len(tiles_to_process) > 0:
        count = process_tiles(pool, tiles_to_process, con, total_tiles, start_time, print_progress, count, delete_vanished_tiles)

    if print_progress:
        sys.stdout.write('\n')

    logger.info("%d tiles finished, %d duplicates ignored (100.0%% @ %.1f tiles/sec)" %
        (count, duplicates, count / (time.time() - start_time)))
    if print_progress:
        sys.stdout.write("%d tiles finished, %d duplicates ignored (100.0%% @ %.1f tiles/sec)\n" %
            (count, duplicates, count / (time.time() - start_time)))
        sys.stdout.flush()


    pool.close()

    logger.debug("Dropping index for the tile_id column...")
    con.drop_map_tile_index()
    logger.debug("...done")

    con.optimize_database(kwargs.get('skip_analyze', False), kwargs.get('skip_vacuum', False))
    con.close()
