#!/usr/bin/env python2.7

#import argparse
#import ConfigParser
#import subprocess
#from pprint import pprint #only for development
##from sh import rsync

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
    settings = {'plugindir': "plugins",
                'basepath': ".",
                'currentdbdir': "databases",
                'previousdbdir': "previous",
                'pendingdbdir': "pending",
                'log_file': "default.log", 
                'markerdownloaded': "FINISHED_DOWNLOAD", 
                'markerwontupdate': "WILL_NOT_UPDATE", 
                'markerupdate': "UPDATEME", 
                'markerupdatestable': "UPDATEME_STABLE", 
                'databases': "databases",
                'data': "data",
    }
    return settings

#def check_finished_dl(dbname, datadir, datadbupdate, fldownloaded, flwontupdate):
#    flagdl = os.path.join(datadbupdate, fldownloaded)
#    flagwu = os.path.join(datadbupdate, flwontupdate)
#    if os.path.isdir(datadbupdate):
#    if os.path.isfile(flagwu):
#        os.remove(flagwu)
#    if os.path.isfile(flagdl):
#        shutil.rmtree(t2dbpath)
#        os.remove(flagdl)
#        shutil.move(t1dbpath, t2dir)
#        shutil.move(t0dbpath, t1dir)
#        os.makedirs(t0dbpath)
#    return


def update_latest(run, data, databases, dbname, latest, stable, previous, update, fldownloaded, flwontupdate):
    ldir = os.readlink(os.path.join(databases, dbname, latest))
    sdir = os.readlink(os.path.join(databases, dbname, stable))
    pdir = os.readlink(os.path.join(databases, dbname, prevous))
    ndir = os.readlink(os.path.join(data, '{}-{}'.format(dbname, update)))
    if ldir == sdir or ldir == pdir:
        shutil.copytree(ldir, ndir)
    else: 
        shutil.move(ldir, ndir)
    print "USE THREADS"
    run(ndir, fldownloaded, flwontupdate)
    
    return

#def check_incomplete_dl(dbname, t0dir, markerdownloaded):
#    t0dbpath = os.path.join(t0dir, dbname)
#    flagdl = os.path.join(t0dbpath, markerdownloaded)
#    if not os.path.isfile(flagdl):
#        shutil.rmtree(t0dbpath)
#        os.makedirs(t0dbpath)
#    return


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
    data = os.path.join(basepath, settings['data'])
    databases = os.path.join(basepath, settings['databases'])
    fldownloaded = settings['markerdownloaded']
    flwontupdate = settings['markerwontupdate']
    flupdate = settings['markerupdate']
    flupdatestable = settings['markerupdatestable']
   
    # our plugins directory
    f = open('{}/{}'.format(plugindir, '__init__.py'), 'w').close()
    import plugins

    #set up logging and scheduler
    logging.basicConfig()
    scheduler = BackgroundScheduler()

    try:
        plugins = update_plugin_list(plugindir)
        runscr = {}
        person = {}
        email = {}
        flagdownloaded = []
        for i, e in enumerate(plugins):
            module = importlib.import_module('plugins.{}'.format(e))
            update = getattr(module, 'check_update_daily')
            runscr[e] = getattr(module, 'run')
            second = getattr(module, 'second')
            minute = getattr(module, 'minute')
            hour = getattr(module, 'hour')
            doweek = getattr(module, 'day_of_week')
            person[e] = getattr(module, 'person')
            email[e] = getattr(module, 'email')
            datadbupdate = os.path.join(data, '{}-updating'.format(e))
            #flagdownloaded.append(os.path.join(pendingdb, settings['markerdownloaded']))

 
            print e, second, minute, hour, doweek
            
            LATEST = os.path.join(databases, e, 'latest')
            UPDATE = os.path.join(data, '{}-updating'.format(e))
            arguments = [UPDATE, LATEST, flupdate]
            scheduler.add_job(update, 'cron', args = arguments, name = e,
                day_of_week = doweek, hour = hour, minute = minute, second = second)

        #for i, e in enumerate(plugins):
            # cleaning pass: check for pending complete download
            #check_finished_dl(e, pendingdbdir, currentdbdir, previousdbdir, settings['markerdownloaded'])
            # cleaning pass: check for incomplete download
            #check_incomplete_dl(e, pendingdbdir, settings['markerdownloaded'])
        
        scheduler.start()
        while True:
            time.sleep(2)
            fo = open('schedulerjobs.log', 'w')
            scheduler.print_jobs(out = fo)
            fo.close()
            status = {}
            for i, e in enumerate(plugins):

                dlstatus = 'up_to_date'
                UPDATEDIR = os.path.join(data, '{}-updating'.format(e))

                # there is a db to update
                updateme = os.path.join(UPDATEDIR, flupdate)
                if os.path.isfile(updateme):
                    shutil.rmtree(UPDATEDIR)
                    update_latest(runscr[e], data, databases, e, 'latest', 'stable', 'previous', 'updating', fldownloaded, flwontupdate)
                    dlstatus = 'updating'

                # there is not db to update
                dont_updateme = os.path.join(UPDATEDIR, flwontupdate)
                if os.path.isfile(dont_updateme):
                    shutil.rmtree(UPDATEDIR)

                # is there a db updating?
                if os.path.exists(UPDATEDIR):
                    dlstatus = 'updating'

                # finished downloading. m vdirectories, update symlink.
                downloaded = os.path.join(UPDATEDIR, fldownloaded)
                if os.path.isfile(downloaded):
                    os.remove(downloaded)
                    timestr = time.strftime("%H%M%S", time.localtime())
                    ndir = os.path.join(data, '{}-{}'.format(e, tstr))
                    shutil.move(UPDATEDIR, ndir)
                    os.symlink(ndir, LATEST)

                # status
                status[e] = dict(
                    status = dlstatus,
                    nextupdate = update_nextupdate(e, open('schedulerjobs.log', 'r')),
                    person = person[e],
                    email = email[e]
                )

            write_status(status, 'status.log')
            
    except (KeyboardInterrupt, SystemExit):
         scheduler.shutdown()
#        logger.info('Cancelled')

