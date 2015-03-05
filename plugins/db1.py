import time
import os

#schedule
day_of_week='*'
hour='*'
minute='*'
second='*/7'


def run(PATH, FLAGFINISHED):
    
    print("DB 1: Input arguments")
    print('{}/{}'.format(PATH, FLAGFINISHED))
    print("DB 1: START Downloading")
    f = open('{}/DATABASE-{}'.format(PATH, time.time()), 'w').close()
    time.sleep(4)
    print("DB 1: FININSHED Download")
    f = open('{}/{}'.format(PATH, FLAGFINISHED), 'w').close()
