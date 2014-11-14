import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib, tempfile, multiprocessing

from util import mbtiles_connect, prettify_connect_string, flip_y
from multiprocessing import Pool

logger = logging.getLogger(__name__)


def test_tile(next_tile):
    tile_file_path, command_list, revert_test = next_tile['filename'], next_tile['command_list'], next_tile['revert_test']

    if command_list == None or tile_file_path == None or not os.path.isfile(tile_file_path):
        return next_tile

    command = command_list[0]
    result  = 0

    # logger.debug("Executing command: %s" % command)

    # Common shortcuts
    if command == "false" or command.startswith("false "):
        result = 1
    elif command == "true" or command.startswith("true "):
        result = 0
    else:
        result = os.system(command % (tile_file_path))

    if (revert_test == False and result != 0) or (revert_test == True and result == 0):
        next_tile['result'] = "/%s/%s/%s.%s\n" % (next_tile['tile_z'], next_tile['tile_x'], next_tile['tile_y'], next_tile['format'])
    else:
        next_tile['result'] = None

    return next_tile


def process_tiles(pool, tiles_to_process):
    # Execute commands in parallel
    # logger.debug("Starting multiprocessing...")
    processed_tiles = pool.map(test_tile, tiles_to_process)

    for next_tile in processed_tiles:
        if next_tile['result']:
            sys.stderr.write(next_tile['result'])

        tile_file_path = next_tile['filename']
        os.remove(tile_file_path)


def test_mbtiles(mbtiles_file, **kwargs):

    scale       = kwargs.get('tile_scale', None)
    zoom        = kwargs.get('zoom', -1)
    min_zoom    = kwargs.get('min_zoom', 0)
    max_zoom    = kwargs.get('max_zoom', 18)
    tmp_dir     = kwargs.get('tmp_dir', None)
    flip_tile_y = kwargs.get('flip_y', False)

    min_timestamp    = kwargs.get('min_timestamp', 0)
    max_timestamp    = kwargs.get('max_timestamp', 0)
    revert_test     = kwargs.get('revert_test', False)

    auto_commit     = kwargs.get('auto_commit', False)
    journal_mode    = kwargs.get('journal_mode', 'wal')
    synchronous_off = kwargs.get('synchronous_off', False)

    default_pool_size = kwargs.get('poolsize', -1)

    if zoom >= 0:
        min_zoom = max_zoom = zoom


    con = mbtiles_connect(mbtiles_file, auto_commit, journal_mode, synchronous_off, False, True)


    zoom_level_string = None

    if min_zoom == max_zoom:
        zoom_level_string = "zoom level %d" % (min_zoom)
    else:
        zoom_level_string = "zoom levels %d -> %d" % (min_zoom, max_zoom)

    logger.info("Testing %s (%s)" % (prettify_connect_string(con.connect_string), zoom_level_string))


    image_format = 'png'

    metadata = con.metadata()
    if metadata.has_key('format'):
        image_format = metadata['format']


    if default_pool_size < 1:
        default_pool_size = None
        logger.debug("Using default pool size")
    else:
        logger.debug("Using pool size = %d" % (default_pool_size))

    pool = Pool(default_pool_size)
    multiprocessing.log_to_stderr(logger.level)


    chunk = 1000
    tiles_to_process = []

    for t in con.tiles(min_zoom, max_zoom, min_timestamp, max_timestamp, scale):
        tile_z = t[0]
        tile_x = t[1]
        tile_y = t[2]
        tile_scale = t[3]
        tile_data = str(t[4])

        if flip_tile_y:
            tile_y = flip_y(tile_z, tile_y)

        tmp_file_fd, tmp_file_name = tempfile.mkstemp(suffix=".%s" % (image_format), prefix="tile_", dir=tmp_dir)
        tmp_file = os.fdopen(tmp_file_fd, "w")
        tmp_file.write(tile_data)
        tmp_file.close()

        tiles_to_process.append({
            'tile_x' : tile_x,
            'tile_y' : tile_y,
            'tile_z' : tile_z,
            'filename' : tmp_file_name,
            'format' : image_format,
            'revert_test' : revert_test,
            'command_list' : kwargs.get('command_list', [])
        })

        if len(tiles_to_process) < chunk:
            continue

        process_tiles(pool, tiles_to_process)

        tiles_to_process = []


    if len(tiles_to_process) > 0:
        process_tiles(pool, tiles_to_process)


    pool.close()
    con.close()
