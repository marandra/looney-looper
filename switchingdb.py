#!/usr/bin/env python2.7

#import argparse
#import ConfigParser
#import subprocess
#from pprint import pprint #only for development
##from sh import rsync
#import tiff2jp2 #only for development

import logging
import logging.handlers
from apscheduler.schedulers.background import BackgroundScheduler
import time
import sys
import os
import shutil
import importlib
import glob

################################################################################
def update_plugin_list(pluginsdir):
    fi = open(os.path.join(pluginsdir, '__init__.py'), 'w')
    modules = glob.glob(os.path.join(pluginsdir,'*.py'))
    all = [os.path.basename(f)[:-3] for f in modules]
    all.remove('__init__')
    fi.write('__all__ = {}'.format(all))
    fi.close
    reload(plugins)
    return all


def get_settings():
    #defaults
    configfile = './datamover.ini'
    sectname = 'config'
    settings = {'plugindir':     "plugins",
                'basepath':      ".",
                'currentdbdir':  "databases",
                'previousdbdir': "previous",
                'pendingdbdir':  "pending",
                'log_file':      "default.log", 
                'marker':        "FINISHED_DOWNLOAD", 
    }
    return settings


def check_finished_dl(dbname, t0dir, t1dir, t2dir, marker):
    t0dbpath = os.path.join(t0dir, dbname)
    t1dbpath = os.path.join(t1dir, dbname)
    t2dbpath = os.path.join(t2dir, dbname)
    flagdl = os.path.join(t0dbpath, marker)
    flagwu = os.path.join(t0dbpath, 'WILL_NOT_UPDATE')
    if os.path.isfile(flagwu):
        os.remove(flagwu)
    if os.path.isfile(flagdl):
        #artifical delay. REMOVE IT.
        time.sleep(1)
        shutil.rmtree(t2dbpath)
        os.remove(flagdl)
        shutil.move(t1dbpath, t2dir)
        shutil.move(t0dbpath, t1dir)
        os.makedirs(t0dbpath)
    return


def check_incomplete_dl(dbname, t0dir, marker):
    t0dbpath = os.path.join(t0dir, dbname)
    flagdl = os.path.join(t0dbpath, marker)
    if not os.path.isfile(flagdl):
        shutil.rmtree(t0dbpath)
        os.makedirs(t0dbpath)
    return


def update_status(dbname, t0dir):
    status = 'up_to_date'
    if os.listdir(os.path.join(t0dir, dbname)):
        status = 'updating'
    return status


def update_nextupdate(dbname, fj):
    for line in fj:
        if line.split()[0] == dbname:
            time = ' '.join(line.split()[9:12])[:-1]
    return time


def write_status(statusdict, fname):
    fo = open(fname, 'w')
    line = []
    line.append('BC2 Data    {}\n'.format(
        time.strftime("%d %b %Y %H:%M:%S", time.localtime())))
    line.append('Live data directory: /import/bc2/data/databases\n\n')
    line.append('{:<10s}{:<13s}{:<26s}{:<19}{:<60s}\n\n'.format(
        'Target',
        'Status',
        'Next update',
        'Responsable',
        'Email'))
    fo.write(''.join(line))
    for key, val in status.iteritems():
        line = '{:<10s}{:<13s}{:<26s}{:<19}{:<60s}\n'.format(
            key,
            val['status'],
            val['nextupdate'],
            val['person'],
            val['email'])
        fo.write(line)
    return


#######################################################################
#main
if __name__ == "__main__":

    #set up options
    settings = get_settings()
    basepath = settings['basepath']
    plugindir = os.path.join(basepath, settings['plugindir'])
    currentdbdir = os.path.join(basepath, settings['currentdbdir'])
    previousdbdir = os.path.join(basepath, settings['previousdbdir'])
    pendingdbdir = os.path.join(basepath, settings['pendingdbdir'])
   
    # our plugins directory
    f = open('{}/{}'.format(plugindir, '__init__.py'), 'w').close()
    import plugins

    #set up logging and scheduler
    logging.basicConfig()
    scheduler = BackgroundScheduler()

    try:
        scheduler.start()
        plugins = update_plugin_list(plugindir)
        person = {}
        email = {}
        flagdownloaded = []
        for i, e in enumerate(plugins):
            module = importlib.import_module('plugins.{}'.format(e))
            runscr = getattr(module, 'run')
            second = getattr(module, 'second')
            minute = getattr(module, 'minute')
            hour = getattr(module, 'hour')
            doweek = getattr(module, 'day_of_week')
            person[e] = getattr(module, 'person')
            email[e] = getattr(module, 'email')
            pendingdb = os.path.join(pendingdbdir, e)
            previousdb = os.path.join(previousdbdir, e)
            currentdb = os.path.join(currentdbdir, e)
            flagdownloaded.append(os.path.join(pendingdb, settings['marker']))
            
            print e, second, minute, hour, doweek
            arguments = [pendingdb, settings['marker']]
            #create db dirs if don't exist
            if not os.path.exists(pendingdb):
                os.makedirs(pendingdb)
            if not os.path.exists(currentdb):
                os.makedirs(currentdb)
            if not os.path.exists(previousdb):
                os.makedirs(previousdb)
            # add jobs to scheduler
            scheduler.add_job(runscr, 'cron', args = arguments, name = e,
                day_of_week = doweek, hour = hour, minute = minute, second = second)

        for i, e in enumerate(plugins):
            # cleaning pass: check for pending complete download
            check_finished_dl(e, pendingdbdir, currentdbdir, previousdbdir, settings['marker'])
            # cleaning pass: check for incomplete download
            check_incomplete_dl(e, pendingdbdir, settings['marker'])
        
        while True:
            time.sleep(2)
            fo = open('schedulerjobs.log', 'w')
            scheduler.print_jobs(out = fo)
            fo.close()
            status = {}
            for i, e in enumerate(plugins):
                status[e] = dict(
                    status = update_status(e, pendingdbdir),
                    nextupdate = update_nextupdate(e, open('schedulerjobs.log', 'r')),
                    person = person[e],
                    email = email[e]
                )
            write_status(status, 'status.log')
            for i, e in enumerate(plugins):
                 check_finished_dl(e, pendingdbdir, currentdbdir, previousdbdir, settings['marker'])
            
    except (KeyboardInterrupt, SystemExit):
         scheduler.shutdown()
#        logger.info('Cancelled')

