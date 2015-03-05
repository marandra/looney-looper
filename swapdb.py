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
        print 'Remove ' + t2dbpath
        shutil.rmtree(t2dbpath)
        print 'Remove ' + flagdl
        os.remove(flagdl)
        print 'Move ' + t1dbpath + ' '+ t2dir
        shutil.move(t1dbpath, t2dir)
        print 'Move ' + t0dbpath + ' '+ t1dir
        shutil.move(t0dbpath, t1dir)
        print 'Make dir ' + t0dbpath
        os.makedirs(t0dbpath)
    return


def check_incomplete_dl(dbname, t0dir, marker):
    t0dbpath = os.path.join(t0dir, dbname)
    flagdl = os.path.join(t0dbpath, marker)
    if not os.path.isfile(flagdl):
        print 'Remove ' + t0dbpath
        shutil.rmtree(t0dbpath)
        print 'Make dir ' + t0dbpath
        os.makedirs(t0dbpath)
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

    #initial plugin scan
#    pluginlist = get_plugins(plugindir)
#
#    for i in pluginlist:
#        p = imp.load_module('__init__.py', *i['info'])
#        pathpending = os.path.join(basepath, settings['pending'], i['name'])
#        if not os.path.exists(pathpending):
#            os.makedirs(pathpending) 
#        print "LOADED " + i['name'], pathpending, p, MARKER
#        scheduler.add_job(lambda: p.run(pathpending, MARKER), 'cron', id = i['name'],
#            day_of_week = p.day_of_week,
#            hour = p.hour,
#            minute = p.minute, 
#            second = p.second)

    try:
        scheduler.start()
    #    lo = []
    #    plugins = {}
    #    while True:
    #        ln = glob.glob(os.path.join(plugindir, '*'))
    #        #detect new plugins
    #        ae = newelem(ln, lo)
    #        for i in ae:
    #            #check valid plugin
    #            if not os.path.isdir(i) or not '__init__.py' in os.listdir(i):
    #                continue
    #            name = os.path.split(i)[-1]
    #            #fd, fn, desc = imp.find_module('__init__', [i])
    #            print "Loading: " + name
    #            #p = imp.load_module('__init__.py', fd, fn, desc)
    #            #prun = getattr(importlib.import_module(ii),'run')
    #            prun = getattr(importlib.import_module(os.path.join(i,'__init__.py')), 'run')
    #            pathpending = os.path.join(basepath, settings['pending'], name)
    #            #add to dict
    #            scheduler.add_job( lambda: prun(pathpending, MARKER), 
    #                'cron', id = name, second = 12)
    #            #plugins[i] = {'plugin': p, 'path': pathpending,
    #            #    'marker': MARKER, 'name': name}
    #            lo.append(i)
    #            #print plugins[i]['plugin']
    #        #detect deleted plugins
    #        re = [x for x in lo if x not in ln]
    #        for i in re:
    #            print "Removed: " + i
    #            #name = os.path.split(i)[-1]
    #            #unload scheduler
    #            lo.remove(i)
    #        #scheduler.print_jobs()
    #        #for i in ae:
    #        #    #p = plugins[i]['plugin']
    #        #    #pathpending = plugins[i]['path']
    #        #    #MARKER = plugins[i]['marker']
    #        #    #name = plugins[i]['name']
    #        #    scheduler.add_job( lambda: p.run(pathpending, MARKER), 
    #        #        'cron', id = name, second = p.second)
    #        #    print "Scheduled: " + name
    #        #    print p
        plugins = update_plugin_list(plugindir)
        mod = []
        run = []
        sec = []
        min = []
        hrs = []
        dow = []
        pendingdb = []
        previousdb = []
        currentdb = []
        flagdownloaded = []
        for i, e in enumerate(plugins):
            print i, e
            mod.append(importlib.import_module('plugins.{}'.format(e.split()[0])))
            run.append(getattr(mod[-1], 'run'))
            sec.append(getattr(mod[-1], 'second'))
            min.append(getattr(mod[-1], 'minute'))
            hrs.append(getattr(mod[-1], 'hour'))
            dow.append(getattr(mod[-1], 'day_of_week'))
            pendingdb.append(os.path.join(pendingdbdir, e.split()[0]))
            previousdb.append(os.path.join(previousdbdir, e.split()[0]))
            currentdb.append(os.path.join(currentdbdir, e.split()[0]))
            flagdownloaded.append(os.path.join(pendingdb[i], settings['marker']))
            
            print e, dow[i], hrs[i], min[i], sec[i]
            arguments = [pendingdb[i], settings['marker']]
            print arguments
            #create db dirs if don't exist
            if not os.path.exists(pendingdb[i]):
                os.makedirs(pendingdb[i])
            if not os.path.exists(currentdb[i]):
                os.makedirs(currentdb[i])
            if not os.path.exists(previousdb[i]):
                os.makedirs(previousdb[i])
            # add jobs to scheduler
            scheduler.add_job(run[i], 'cron', args = arguments, name = e,
                day_of_week = dow[i], hour = hrs[i], minute = min[i], second = sec[i])

        # cleaning pass: check for pending complete download
        for i, e in enumerate(plugins):
             check_finished_dl(e, pendingdbdir, currentdbdir, previousdbdir, settings['marker'])
        # cleaning pass: check for incomplete download
        for i, e in enumerate(plugins):
             check_incomplete_dl(e, pendingdbdir, settings['marker'])

        while True:
            time.sleep(1)
            fo = open('schedulerjobs.log', 'w')
            scheduler.print_jobs(out = fo)
            #scheduler.print_jobs()
            fo.close()
            #print "tic"
            for i, e in enumerate(plugins):
                 check_finished_dl(e, pendingdbdir, currentdbdir, previousdbdir, settings['marker'])
            
    except (KeyboardInterrupt, SystemExit):
#         pass
         scheduler.shutdown()
#        logger.info('Cancelled')

