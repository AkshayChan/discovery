# GLOBAL VARIABLES file
import logging
import sys
global GROUPBY_COLS, logger

GROUPBY_COLS = ['eventid', 'raceid', 'racetype', 'gender', 'heat', 'round']

stdout_handler = logging.StreamHandler(sys.stdout)
logging.basicConfig(
    level=logging.INFO, 
    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
    handlers=[stdout_handler]
)
logger = logging.getLogger('root')