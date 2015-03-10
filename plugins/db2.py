import time
import os

#config schedule and contact data
second = '*/30'
minute = '*'
hour = '*'
day_of_week = '*'
stable_second = '*/80'
stable_minute = '*'
stable_hour = '*'
stable_day_of_week = '*'
person='Fredric Clayborn'
email='fredric.clayborn@unibas.ch'


def check_update_stable(PATH, LATEST, FLAGUPDATESTABLE):
    os.makedirs(PATH)
    open('{}/{}'.format(PATH, FLAGUPDATESTABLE), 'w').close()
    return


def check_update_daily(PATH, LATEST, FLAGUPDATE):
    os.makedirs(PATH)

    #UPDATE = check_update(PATH, LATEST)
    UPDATE = True

    if UPDATE:
        open('{}/{}'.format(PATH, FLAGUPDATE), 'w').close()
    else:
        open('{}/{}'.format(PATH, FLAGWONTUPDATE), 'w').close()
    return


def run(PATH, FLAGFINISHED, FLAGWONTUPDATE):
    print 'PATH: ' + PATH
    open('{}/DATABASE_2'.format(PATH), 'w').close()
    time.sleep(5)

    open('{}/{}'.format(PATH, FLAGFINISHED), 'w').close()
    return 

############################################

if __name__ == '__main__':
    run('../pending/db1', 'FINISHED_DOWNLOAD')
