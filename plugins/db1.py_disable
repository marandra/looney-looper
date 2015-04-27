import time
import os
import logging

#config schedule and contact data
second = '*/20'
minute = '*'
hour = '*'
day_of_week = '*'
stable_second = '*/65'
stable_minute = '*'
stable_hour = '*'
stable_day_of_week = '*'
person = 'Ross Mccants'
email = 'ross.mccants@unibas.ch'

logger = logging.getLogger(__name__)

def check_update_stable(PATH, LATEST, FLAG_UPDATE_STABLE):
    os.makedirs(PATH)
    open(os.path.join(PATH, FLAG_UPDATE_STABLE), 'w').close()
    logger.debug('Created STABLE {}'.format(os.path.join(PATH, FLAG_UPDATE_STABLE)))
    return


def check_update_daily(PATH, LATEST, FLAG_UPDATE, FLAG_WONT_UPDATE):
    os.makedirs(PATH)

    #UPDATE = check_update(PATH, LATEST)
    time.sleep(1)
    UPDATE = True

    if UPDATE:
        open(os.path.join(PATH, FLAG_UPDATE), 'w').close()
        logger.debug('Created DAILY {}'.format(os.path.join(PATH, FLAG_UPDATE)))
    else:
        open(os.path.join(PATH, FLAG_WONT_UPDATE), 'w').close()
        logger.debug('Created DAILY {}'.format(os.path.join(PATH, FLAG_WONT_UPDATE)))
        logger.info('Now new updates')
    return


def run(PATH, FLAG_FINISHED):
    logger.info('Path to update: ' + PATH)
    open('{}/DATABASE_1'.format(PATH), 'w').close()
    time.sleep(5)

    open('{}/{}'.format(PATH, FLAG_FINISHED), 'w').close()
    return 

############################################

if __name__ == '__main__':
    run('../pending/db1', 'FINISHED_DOWNLOAD')
