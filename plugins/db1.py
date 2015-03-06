import time
import os

#config schedule and contact data
second = '*/21'
minute = '*'
hour = '*'
day_of_week = '*'
person = 'Ross Mccants'
email = 'ross.mccants@unibas.ch'


def run(PATH, FLAGFINISHED):

    ######## BEGINING OF DOWNLOAD SCRIPT ########
    #update = check_update()
    UPDATE = False
    if UPDATE:
        timestr = time.strftime("%H:%M:%S", time.localtime())
        open('{}/DATABASE_1-{}'.format(PATH, timestr), 'w').close()
        time.sleep(5)
    ######## END OF DOWNLOAD SCRIPT ########

    if UPDATE:
        open('{}/{}'.format(PATH, FLAGFINISHED), 'w').close()
    else:
        open('{}/{}'.format(PATH, 'WILL_NOT_UPDATE'), 'w').close()
    return 

############################################

if __name__ == '__main__':
    run('../pending/db1', 'FINISHED_DOWNLOAD')
