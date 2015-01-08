import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib, tempfile

from util import mbtiles_connect, execute_commands_on_tile, flip_y, prettify_connect_string

logger = logging.getLogger(__name__)


def disk_to_mbtiles(directory_path, mbtiles_file, **kwargs):

    auto_commit     = kwargs.get('auto_commit', False)
    journal_mode    = kwargs.get('journal_mode', 'wal')
    synchronous_off = kwargs.get('synchronous_off', False)

    print_progress  = kwargs.get('progress', False)
    flip_tile_y     = kwargs.get('flip_y', False)

    scale    = kwargs.get('tile_scale', None)
    zoom     = kwargs.get('zoom', -1)
    min_zoom = kwargs.get('min_zoom', 0)
    max_zoom = kwargs.get('max_zoom', 18)
    tmp_dir  = kwargs.get('tmp_dir', None)

    if tmp_dir and not os.path.isdir(tmp_dir):
        os.mkdir(tmp_dir)

    if zoom >= 0:
        min_zoom = max_zoom = zoom


    con = mbtiles_connect(mbtiles_file, auto_commit, journal_mode, synchronous_off, False)

    con.mbtiles_setup()

    # if not con.is_compacted():
    #     con.close()
    #     logger.info("The mbtiles database must be compacted, exiting...")
    #     return

    con.mbtiles_setup()


    zoom_level_string = None

    if min_zoom == max_zoom:
        zoom_level_string = "zoom level %d" % (min_zoom)
    else:
        zoom_level_string = "zoom levels %d -> %d" % (min_zoom, max_zoom)

    logger.info("Importing path:'%s' --> %s (%s)" % (directory_path, prettify_connect_string(con.connect_string), zoom_level_string))


    image_format = 'png'
    try:

        metadata = json.load(open(os.path.join(directory_path, 'metadata.json'), 'r'))
        image_format = metadata.get('format', 'png')

        # Check that the old and new image formats are the same
        receiving_metadata = con.metadata()

        if receiving_metadata != None and len(receiving_metadata) > 0:
            original_format = receiving_metadata.get('format')

            if original_format != None and image_format != original_format:
                sys.stderr.write('The databases to merge must use the same image format (png or jpg)\n')
                sys.exit(1)

        for name, value in metadata.items():
            con.update_metadata(name, value)

        logger.info('metadata from metadata.json restored')

    except IOError:
        logger.warning('metadata.json not found')


    count = 0
    start_time = time.time()

    if print_progress:
        sys.stdout.write("0 tiles imported (0 tiles/sec)")
        sys.stdout.flush()


    known_tile_ids = set()

    tmp_images_list = []
    tmp_row_list = []
    tmp_tiles_list = []

    for r1, zs, ignore in os.walk(os.path.join(directory_path, "tiles")):
        for tile_z in zs:
            if int(tile_z) < min_zoom or int(tile_z) > max_zoom:
                continue

            for r2, xs, ignore in os.walk(os.path.join(r1, tile_z)):
                for tile_x in xs:
                    for r2, ignore, ys in os.walk(os.path.join(r1, tile_z, tile_x)):
                        for tile_y in ys:
                            tile_y, extension = tile_y.split('.')

                            f = open(os.path.join(r1, tile_z, tile_x, tile_y) + '.' + extension, 'rb')
                            tile_data = f.read()
                            f.close()

                            if flip_tile_y:
                                tile_y = str(flip_y(tile_z, tile_y))

                            # Execute commands
                            if kwargs.get('command_list'):
                                tile_data = execute_commands_on_tile(kwargs['command_list'], image_format, tile_data, tmp_dir)

                            if con.is_compacted():
                                m = hashlib.md5()
                                m.update(tile_data)
                                tile_id = m.hexdigest()

                                if tile_id not in known_tile_ids:
                                    tmp_images_list.append( (tile_id, tile_data) )
                                    known_tile_ids.add(tile_id)

                                tmp_row_list.append( (tile_z, tile_x, tile_y, 1, tile_id, int(time.time())) )
                            else:
                                tmp_tiles_list.append( (tile_z, tile_x, tile_y, 1, tile_data, int(time.time())) )

                            count = count + 1
                            if (count % 100) == 0:
                                logger.debug("%d tiles imported (%.1f tiles/sec)" % (count, count / (time.time() - start_time)))
                                if print_progress:
                                    sys.stdout.write("\r%d tiles imported (%.1f tiles/sec)" % (count, count / (time.time() - start_time)))
                                    sys.stdout.flush()

                            if len(tmp_images_list) > 250:
                                con.insert_tiles_to_images(tmp_images_list)
                                tmp_images_list = []

                            if len(tmp_row_list) > 250:
                                con.insert_tiles_to_map(tmp_row_list)
                                tmp_row_list = []

                            if len(tmp_tiles_list) > 250:
                                con.insert_tiles(tmp_tiles_list)
                                tmp_tiles_list = []


    # Push the remaining rows to the database
    if len(tmp_images_list) > 0:
        con.insert_tiles_to_images(tmp_images_list)

    if len(tmp_row_list) > 0:
        con.insert_tiles_to_map(tmp_row_list)

    if len(tmp_tiles_list) > 0:
        con.insert_tiles(tmp_tiles_list)

    if print_progress:
        sys.stdout.write('\n')

    logger.info("%d tiles imported." % (count))
    if print_progress:
        sys.stdout.write("%d tiles imported.\n" % (count))
        sys.stdout.flush()


    con.optimize_database(kwargs.get('skip_analyze', False), kwargs.get('skip_vacuum', False))

    con.close()
