import time
import os

#config schedule and contact data
second = '*/20'
minute = '*'
hour = '*'
day_of_week = '*'
stable_second = '*/20'
stable_minute = '*'
stable_hour = '*'
stable_day_of_week = '*'
person = 'Ross Mccants'
email = 'ross.mccants@unibas.ch'


def check_update_stable(PATH, FLAGUPDATESTABLE):
    open('{}/{}'.format(PATH, FLAGUPDATESTABLE), 'w').close()
    return


def check_update_daily(PATH, LATEST, FLAGUPDATE):
    #UPDATE = check_update(PATH, LATEST)
    UPDATE = True
    if not os.path.exists(PATH):
        os.makedirs(PATH)
    if UPDATE:
        open('{}/{}'.format(PATH, FLAGUPDATE), 'w').close()
    else:
        open('{}/{}'.format(PATH, FLAGWONTUPDATE), 'w').close()
    return


def run(PATH, FLAGFINISHED, FLAGWONTUPDATE):
    print 'PATH: ' + PATH
    open('{}/DATABASE_1'.format(PATH), 'w').close()
    time.sleep(5)

    open('{}/{}'.format(PATH, FLAGFINISHED), 'w').close()
    return 

############################################

if __name__ == '__main__':
    run('../pending/db1', 'FINISHED_DOWNLOAD')
