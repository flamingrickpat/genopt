import datetime
import logging
import random
import traceback
import dm_worker
import file_utils
from config import Config
from file_utils import alogger

logger = logging.getLogger(__name__)

class WorkItem():
    def __init__(self, config: Config, run: int, id: str, pair: str, timeframe: int, zeitpunkt: int, candles: int):
        self.config = config # config hat guid
        self.run = run # run ist die nr. von kompletten 64-process berechnungsvorgang
        self.id = id # id ist die process id 1-64
        self.pair = pair
        self.timeframe = timeframe
        self.zeitpunkt = zeitpunkt
        self.candles = candles
        self.process = None
        self.start_time = 0

@alogger
def boss_process(config: Config, process: int):
    file_utils.setup_logger_mp(config)

    run = 0
    while True:
        try:
            logging.info(f"Erstelle Work Items von Run Nr. {run}.")

            if config.DATA_BIS == 0:
                bis = int(datetime.datetime.now().timestamp() - (48 * 60 * 60))
            else:
                bis = config.DATA_BIS

            random.seed(file_utils.get_seed())
            pair = random.choice(config.PAIRS)
            tf = random.choice(config.TIMEFRAMES)
            zp = random.randrange(config.DATA_VON, bis)
            cd = random.randrange(config.CANDLES_MIN, config.CANDLES_MAX)

            item = WorkItem(
                config=config,
                run=run,
                id=str(process),
                pair=pair,
                timeframe=tf,
                zeitpunkt=zp,
                candles=cd + 2000
            )

            logging.info(f"WorkItem von Run Nr. {run}: " + str(vars(item)))
            dm_worker.process_item(item)
        except Exception as e:
            logger.error(str(e))
            logger.error(traceback.format_exc())

        run += 1
