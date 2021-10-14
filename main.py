import datetime
import logging
import sys
from multiprocessing import *

import dm_boss
import file_utils
from config import Config
from file_utils import alogger

logger = logging.getLogger(__name__)

@alogger
def main_loop():
    import os
    os.environ['NUMEXPR_MAX_THREADS'] = '1'
    os.environ['NUMEXPR_NUM_THREADS'] = '1'
    config_path = ""
    try:
        config_path = sys.argv[1]
        if not os.path.isfile(config_path):
            logger.error("Config %s gibts nicht." % config_path)
    except:
        logger.error("usage: main.py config_path")
        exit(1)

    config = Config(config_path)
    guid = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    config.GUID = guid
    file_utils.setup_logger(filename=os.path.join(config.LOG_PATH, guid + ".log"))
    logger.info("Starte bot mit GUID %s" % guid)

    processes = []
    if config.WORKER_COUNT == 1:
        dm_boss.boss_process(config, 0)
    else:
        for i in range(config.WORKER_COUNT):
            p = Process(target=dm_boss.boss_process, args=(config, i))
            p.start()
            processes.append(p)

        for p in processes:
            p.join()


if __name__ == '__main__':
    main_loop()