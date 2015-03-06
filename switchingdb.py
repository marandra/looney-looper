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
    settings = {'plugindir':    "plugins",
                'currentdbdir':   "databases",
                'previousdbdir':   "previous",
                'pendingdbdir':    "pending",
                'log_file':   "default.log", 
                'marker':     "FINISHED_DOWNLOAD", 
                'modulename': "__init__",
    }

    return settings



def get_plugins(plugindir):
    plugins = []
    possibleplugins = glob.glob(os.path.join(plugindir, '*'))
    for i in possibleplugins:
        if not os.path.isdir(i) or not '__init__.py' in os.listdir(i):
            continue
        name = os.path.split(i)[-1]
        info = imp.find_module('__init__', [i])
        plugins.append({"name": name, "path": i, "info": info})
    return plugins


def newelem(lnew, l):
    #check added elements
    tmpa = [x for x in lnew if x not in l]
    ea = []
    for i in tmpa:
        # check validity of elements
        ea.append(i)
    return ea


def check_finished_dl(dbname, t0dir, t1dir, t2dir, marker):
    t0dbpath = os.path.join(t0dir, dbname)
    t1dbpath = os.path.join(t1dir, dbname)
    t2dbpath = os.path.join(t2dir, dbname)
    flagdl = os.path.join(t0dbpath, marker)
    if os.path.isfile(flagdl):
        #artifical delay. REMOVE IT.
        time.sleep(1)
        #print 'Remove ' + t2dbpath
        shutil.rmtree(t2dbpath)
        #print 'Remove ' + flagdl
        os.remove(flagdl)
        #print 'Move ' + t1dbpath + ' '+ t2dir
        shutil.move(t1dbpath, t2dir)
        #print 'Move ' + t0dbpath + ' '+ t1dir
        shutil.move(t0dbpath, t1dir)
        #print 'Make dir ' + t0dbpath
        os.makedirs(t0dbpath)
    return


def check_incomplete_dl(dbname, t0dir, marker):
    t0dbpath = os.path.join(t0dir, dbname)
    flagdl = os.path.join(t0dbpath, marker)
    if not os.path.isfile(flagdl):
        #print 'Remove ' + t0dbpath
        shutil.rmtree(t0dbpath)
        #print 'Make dir ' + t0dbpath
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

    basepath = '.'
    plugindir = os.path.join(basepath, settings['plugindir'])
    currentdbdir = os.path.join(basepath, settings['currentdbdir'])
    previousdbdir = os.path.join(basepath, settings['previousdbdir'])
    pendingdbdir = os.path.join(basepath, settings['pendingdbdir'])
   
    # our plugins directory. 'touch plugins/__init__.py' before first run.
    f = open('{}/{}'.format(plugindir, '__init__.py'), 'w').close()
    import plugins

    #set up logging
    logging.basicConfig()

    #set up scheduler
    scheduler = BackgroundScheduler()

    try:
        scheduler.start()
        plugins = update_plugin_list(plugindir)
        module = []
        runscr = []
        second = []
        minute = []
        hour = []
        doweek = []
        person = []
        email = []
        pendingdb = []
        previousdb = []
        currentdb = []
        flagdownloaded = []
        for i, e in enumerate(plugins):
            module.append(importlib.import_module('plugins.{}'.format(e.split()[0])))
            runscr.append(getattr(module[-1], 'run'))
            second.append(getattr(module[-1], 'second'))
            minute.append(getattr(module[-1], 'minute'))
            hour.append(getattr(module[-1], 'hour'))
            doweek.append(getattr(module[-1], 'day_of_week'))
            person.append(getattr(module[-1], 'person'))
            email.append(getattr(module[-1], 'email'))
            pendingdb.append(os.path.join(pendingdbdir, e.split()[0]))
            previousdb.append(os.path.join(previousdbdir, e.split()[0]))
            currentdb.append(os.path.join(currentdbdir, e.split()[0]))
            flagdownloaded.append(os.path.join(pendingdb[i], settings['marker']))
            
            print e, doweek[i], hour[i], minute[i], second[i]
            arguments = [pendingdb[i], settings['marker']]
            #create db dirs if don't exist
            if not os.path.exists(pendingdb[i]):
                os.makedirs(pendingdb[i])
            if not os.path.exists(currentdb[i]):
                os.makedirs(currentdb[i])
            if not os.path.exists(previousdb[i]):
                os.makedirs(previousdb[i])
            # add jobs to scheduler
            scheduler.add_job(runscr[i], 'cron', args = arguments, name = e,
                day_of_week = doweek[i], hour = hour[i], minute = minute[i], second = second[i])

        # cleaning pass: check for pending complete download
        for i, e in enumerate(plugins):
             check_finished_dl(e, pendingdbdir, currentdbdir, previousdbdir, settings['marker'])
        # cleaning pass: check for incomplete download
        for i, e in enumerate(plugins):
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
                    person = person[i],
                    email = email[i]
                )
            write_status(status, 'status.log')
            #print "tic"
            for i, e in enumerate(plugins):
                 check_finished_dl(e, pendingdbdir, currentdbdir, previousdbdir, settings['marker'])
            
    except (KeyboardInterrupt, SystemExit):
#         pass
         scheduler.shutdown()
#        logger.info('Cancelled')

