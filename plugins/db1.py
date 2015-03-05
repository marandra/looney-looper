import time
import os

#schedule
day_of_week='*'
hour='*'
minute='*'
second='*/7'


def run(PATH, FLAGFINISHED):
    
    print("DB 1: Downloading")
    f = open('{}/DATABASE-{}'.format(PATH, time.time()), 'w').close()
    time.sleep(4)
    print("DB 1: Finished download")
    f = open('{}/{}'.format(PATH,FLAGFINISHED), 'w').close()
