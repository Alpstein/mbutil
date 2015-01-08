import sqlite3, uuid, sys, logging, time, os, re, json, zlib, hashlib, tempfile, math

from database import database_connect

logger = logging.getLogger(__name__)


def flip_y(zoom, y):
    return (2**int(zoom)-1) - int(y)


def coordinate_to_tile(longitude, latitude, zoom):
    if latitude > 85.0511:
        latitude = 85.0511
    elif latitude < -85.0511:
        latitude = -85.0511
    latitude_rad = math.radians(latitude)
    n = 2.0 ** zoom
    tileX = int((longitude + 180.0) / 360.0 * n)
    tileY = int((1.0 - math.log(math.tan(latitude_rad) + (1 / math.cos(latitude_rad))) / math.pi) / 2.0 * n)
    if tileX > n-1: tileX = int(n-1)
    return (tileX, tileY)


def tile_to_coordinate(tileX, tileY, zoom):
    n = 2.0 ** zoom
    longitude = (tileX + 0.5) / n * 360.0 - 180.0
    latitude_rad = math.atan(math.sinh(math.pi * (1 - 2 * (tileY + 0.5) / n)))
    latitude = math.degrees(latitude_rad)
    return (longitude, latitude)


def mbtiles_connect(connect_string, auto_commit=False, journal_mode='wal', synchronous_off=False, exclusive_lock=False, check_if_exists=False):
    return database_connect(connect_string, auto_commit, journal_mode, synchronous_off, exclusive_lock, check_if_exists)


def optimize_database(connect_string, auto_commit=False, skip_analyze=True, skip_vacuum=True, journal_mode='wal'):
    con = mbtiles_connect(connect_string, auto_commit, skip_analyze, skip_vacuum, journal_mode)
    con.optimize_database(skip_analyze, skip_vacuum)
    con.close()


def mbtiles_create(connect_string, **kwargs):
    logger.info("Creating empty database %s" % (connect_string))
    con = mbtiles_connect(connect_string)
    con.mbtiles_setup()
    con.close()


def prettify_connect_string(connect_string):
    if connect_string.endswith(".mbtiles"):
        return "mbtiles:'%s'" % (os.path.basename(connect_string))
    elif connect_string.find("driver=postgres") >= 0 or connect_string.startswith("pg:"):
        return "postgres:'%s'" % (re.search('dbname=\'?([^\s\']+)\'?', connect_string).group(1))
    elif connect_string.find("driver=mysql") >= 0 or connect_string.startswith("my:"):
        return "mysql:'%s'" % (re.search('dbname=\'?([^\s\']+)\'?', connect_string).group(1))
    elif connect_string.find("driver=mongodb") >= 0 or connect_string.startswith("mongodb:"):
        return connect_string


def execute_commands_on_tile(command_list, image_format, tile_data, tmp_dir=None):
    if command_list == None or tile_data == None:
        return tile_data

    tmp_file_fd, tmp_file_name = tempfile.mkstemp(suffix=".%s" % (image_format), prefix="tile_", dir=tmp_dir)
    tmp_file = os.fdopen(tmp_file_fd, "w")
    tmp_file.write(tile_data)
    tmp_file.close()

    for command in command_list:
        # logger.debug("Executing command: %s" % command)
        os.system(command % (tmp_file_name))

    tmp_file = open(tmp_file_name, "r")
    new_tile_data = tmp_file.read()
    tmp_file.close()

    os.remove(tmp_file_name)

    return new_tile_data


def execute_commands_on_file(command_list, image_format, image_file_path):
    if command_list == None or image_file_path == None or not os.path.isfile(image_file_path):
        return False

    for command in command_list:
        # logger.debug("Executing command: %s" % command)
        os.system(command % (image_file_path))

    return True


def process_tile(next_tile):
    tile_id, tile_file_path, image_format, command_list = next_tile['tile_id'], next_tile['filename'], next_tile['format'], next_tile['command_list']
    # sys.stderr.write("%s (%s) -> %s\n" % (tile_id, image_format, tile_file_path))

    tile_data = execute_commands_on_file(command_list, image_format, tile_file_path)

    return next_tile
