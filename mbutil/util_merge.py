import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib, tempfile, multiprocessing

from util import mbtiles_connect, execute_commands_on_tile, process_tile, flip_y, prettify_connect_string
from util_check import check_mbtiles
from multiprocessing import Pool

logger = logging.getLogger(__name__)


def process_tiles(pool, tiles_to_process, con, count, total_tiles, start_time, print_progress, delete_vanished_tiles, known_tile_ids):
    tmp_row_list = []

    # Execute commands
    processed_tiles = pool.map(process_tile, tiles_to_process)

    for next_tile in processed_tiles:
        tile_data = None
        tile_id, tile_file_path, original_size, tile_x, tile_y, tile_z, tile_scale = next_tile['tile_id'], next_tile['filename'], next_tile['size'], next_tile['tile_x'], next_tile['tile_y'], next_tile['tile_z'], next_tile['tile_scale']

        if os.path.isfile(tile_file_path):
            tmp_file = open(tile_file_path, "r")
            tile_data = tmp_file.read()
            tmp_file.close()

            os.remove(tile_file_path)

            if tile_data and len(tile_data) > 0:
                m = hashlib.md5()
                m.update(tile_data)
                new_tile_id = m.hexdigest()
                known_tile_ids[tile_id] = new_tile_id

                con.insert_tile_to_images(new_tile_id, tile_data)
                tmp_row_list.append( (tile_z, tile_x, tile_y, tile_scale, new_tile_id, int(time.time())) )
        else:
            if delete_vanished_tiles:
                logger.debug("Deleting vanished tile %s" % (tile_id, ))
                con.expire_tile(tile_z, tile_x, tile_y, tile_scale)
            else:
                logger.error("tile %s vanished!" % (tile_id, ))

        count = count + 1
        if (count % 100) == 0:
            logger.debug("%d tiles merged (%.1f%% @ %.1f tiles/sec)" % (count, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))
            if print_progress:
                sys.stdout.write("\r%d tiles merged (%.1f%% @ %.1f tiles/sec)" % (count, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))
                sys.stdout.flush()

    if len(tmp_row_list) > 0:
        con.insert_tiles_to_map(tmp_row_list)

    return count


def merge_mbtiles(mbtiles_file1, mbtiles_file2, **kwargs):

    scale         = kwargs.get('tile_scale', 1)
    zoom          = kwargs.get('zoom', -1)
    min_zoom      = kwargs.get('min_zoom', 0)
    max_zoom      = kwargs.get('max_zoom', 18)

    tmp_dir         = kwargs.get('tmp_dir', None)
    auto_commit     = kwargs.get('auto_commit', False)
    journal_mode    = kwargs.get('journal_mode', 'wal')
    synchronous_off = kwargs.get('synchronous_off', False)

    min_timestamp = kwargs.get('min_timestamp', 0)
    max_timestamp = kwargs.get('max_timestamp', 0)

    delete_after_export   = kwargs.get('delete_after_export', False)
    print_progress        = kwargs.get('progress', False)
    delete_vanished_tiles = kwargs.get('delete_vanished_tiles', False)
    flip_tile_y           = kwargs.get('flip_y', False)

    if tmp_dir and not os.path.isdir(tmp_dir):
        os.mkdir(tmp_dir)

    if zoom >= 0:
        min_zoom = max_zoom = zoom


    check_before_merge = kwargs.get('check_before_merge', False)
    if check_before_merge and not check_mbtiles(mbtiles_file2, **kwargs):
        sys.stderr.write("The pre-merge check on %s failed\n" % (mbtiles_file2))
        sys.exit(1)


    con1 = mbtiles_connect(mbtiles_file1, auto_commit, journal_mode, synchronous_off, False, False)
    con2 = mbtiles_connect(mbtiles_file2, auto_commit, journal_mode, synchronous_off, False, True)

    con1.mbtiles_setup()

    if not con1.is_compacted():
        con1.close()
        con2.close()
        sys.stderr.write('To merge two mbtiles databases, the receiver must already be compacted\n')
        sys.exit(1)

    if not con2.is_compacted() and (min_timestamp != 0 or max_timestamp != 0):
        con1.close()
        con2.close()
        sys.stderr.write('min-timestamp/max-timestamp can only be used with compacted databases.\n')
        sys.exit(1)


    zoom_level_string = None

    if min_zoom == max_zoom:
        zoom_level_string = "zoom level %d" % (min_zoom)
    else:
        zoom_level_string = "zoom levels %d -> %d" % (min_zoom, max_zoom)

    logger.info("Merging %s --> %s (%s)" % (prettify_connect_string(con2.connect_string), prettify_connect_string(con1.connect_string), zoom_level_string))


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
    chunk = 1000

    total_tiles = con2.tiles_count(min_zoom, max_zoom, min_timestamp, max_timestamp, scale)

    if total_tiles == 0:
        con1.close()
        con2.close()
        sys.stderr.write('No tiles to merge, exiting...\n')
        return

    logger.debug("%d tiles to merge" % (total_tiles))
    if print_progress:
        sys.stdout.write("%d tiles to merge\n" % (total_tiles))
        sys.stdout.write("0 tiles merged (0% @ 0 tiles/sec)")
        sys.stdout.flush()



    # merge and process (--merge --execute)
    if con2.is_compacted() and kwargs['command_list']:
        default_pool_size = kwargs.get('poolsize', -1)
        if default_pool_size < 1:
            default_pool_size = None
            logger.debug("Using default pool size")
        else:
            logger.debug("Using pool size = %d" % (default_pool_size))

        pool = Pool(default_pool_size)
        multiprocessing.log_to_stderr(logger.level)

        tiles_to_process = []
        known_tile_ids = {}

        for t in con2.tiles_with_tile_id(min_zoom, max_zoom, min_timestamp, max_timestamp, scale):
            tile_z = t[0]
            tile_x = t[1]
            tile_y = t[2]
            tile_scale = t[3]
            tile_data = str(t[4])
            tile_id = t[5]

            if flip_tile_y:
                tile_y = flip_y(tile_z, tile_y)

            new_tile_id = known_tile_ids.get(tile_id)
            if new_tile_id is None:
                tmp_file_fd, tmp_file_name = tempfile.mkstemp(suffix=".%s" % (new_format), prefix="tile_", dir=tmp_dir)
                tmp_file = os.fdopen(tmp_file_fd, "w")
                tmp_file.write(tile_data)
                tmp_file.close()

                tiles_to_process.append({
                    'tile_id':tile_id,
                    'filename':tmp_file_name,
                    'format':new_format,
                    'size':len(tile_data),
                    'command_list':kwargs['command_list'],
                    'tile_x':tile_x,
                    'tile_y':tile_y,
                    'tile_z':tile_z,
                    'tile_scale':tile_scale
                })
            else:
                con1.insert_tile_to_map(tile_z, tile_x, tile_y, tile_scale, new_tile_id)

                count = count + 1
                if (count % 100) == 0:
                    logger.debug("%d tiles merged (%.1f%% @ %.1f tiles/sec)" % (count, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))
                    if print_progress:
                        sys.stdout.write("\r%d tiles merged (%.1f%% @ %.1f tiles/sec)" % (count, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))
                        sys.stdout.flush()


            if len(tiles_to_process) < chunk:
                continue

            count = process_tiles(pool, tiles_to_process, con1, count, total_tiles, start_time, print_progress, delete_vanished_tiles, known_tile_ids)

            tiles_to_process = []

        if len(tiles_to_process) > 0:
            count = process_tiles(pool, tiles_to_process, con1, count, total_tiles, start_time, print_progress, delete_vanished_tiles, known_tile_ids)


    # merge from a compacted database (--merge)
    elif con2.is_compacted():
        known_tile_ids = set()

        tmp_images_list = []
        tmp_row_list = []

        for t in con2.tiles_with_tile_id(min_zoom, max_zoom, min_timestamp, max_timestamp, scale):
            tile_z = t[0]
            tile_x = t[1]
            tile_y = t[2]
            tile_scale = t[3]
            tile_data = str(t[4])
            tile_id = t[5]

            if flip_tile_y:
                tile_y = flip_y(tile_z, tile_y)

            if tile_id not in known_tile_ids:
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


    # merge an uncompacted database (--merge)
    else:
        known_tile_ids = set()

        for t in con2.tiles(min_zoom, max_zoom, min_timestamp, max_timestamp, scale):
            tile_z = t[0]
            tile_x = t[1]
            tile_y = t[2]
            tile_scale = t[3]
            tile_data = str(t[4])

            if flip_tile_y:
                tile_y = flip_y(tile_z, tile_y)

            # Execute commands
            if kwargs.get('command_list'):
                tile_data = execute_commands_on_tile(kwargs['command_list'], new_format, tile_data, tmp_dir)

            m = hashlib.md5()
            m.update(tile_data)
            tile_id = m.hexdigest()

            if tile_id not in known_tile_ids:
                con1.insert_tile_to_images(tile_id, tile_data)

            con1.insert_tile_to_map(tile_z, tile_x, tile_y, tile_scale, tile_id)

            known_tile_ids.add(tile_id)

            count = count + 1
            if (count % 100) == 0:
                logger.debug("%d tiles merged (%.1f%% @ %.1f tiles/sec)" % (count, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))
                if print_progress:
                    sys.stdout.write("\r%d tiles merged (%.1f%% @ %.1f tiles/sec)" % (count, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))
                    sys.stdout.flush()


    if print_progress:
        sys.stdout.write('\n')

    logger.info("%d tiles merged (100.0%% @ %.1f tiles/sec)" % (count, count / (time.time() - start_time)))
    if print_progress:
        sys.stdout.write("%d tiles merged (100.0%% @ %.1f tiles/sec)\n" % (count, count / (time.time() - start_time)))
        sys.stdout.flush()


    if delete_after_export:
        logger.debug("WARNING: Removing merged tiles from %s" % (mbtiles_file2))

        con2.delete_tiles(min_zoom, max_zoom, min_timestamp, max_timestamp, scale)
        con2.optimize_database(kwargs.get('skip_analyze', False), kwargs.get('skip_vacuum', False))


    con1.close()
    con2.close()
