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
import threading

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
    settings = {
                'basepath': "/home/marcelo/Projects/switching-db",
                'plugindir': "plugins",
                'databases': "databases",
                'data': "data",
                'markerupdated': "FINISHED_DOWNLOAD", 
                'markerwontupdate': "WILL_NOT_UPDATE", 
                'markerupdate': "UPDATEME", 
                'markerupdatestable': "UPDATEME_STABLE", 
                'log_file': "default.log", 
    }
    return settings


def update_latest(run, data, databases, dbname, latest, stable, previous, update, fldownloaded, flwontupdate):
    ldir = os.readlink(os.path.join(databases, dbname, latest))
    ndir = os.path.join(data, '{}-{}'.format(dbname, update))
    shutil.copytree(ldir, ndir)
    run_thread = threading.Thread(target=run, args=[ndir, fldownloaded, flwontupdate])
    run_thread.start()
    return


#def check_incomplete_dl(dbname, t0dir, markerdownloaded):
#    t0dbpath = os.path.join(t0dir, dbname)
#    flagdl = os.path.join(t0dbpath, markerdownloaded)
#    if not os.path.isfile(flagdl):
#        shutil.rmtree(t0dbpath)
#        os.makedirs(t0dbpath)
#    return

def update_status(statusdict, fname, fsched):
    # header 
    fo = open(fname, 'w')
    fs = open(fsched, 'r')
    timestr = time.strftime("%d %b %Y %H:%M:%S", time.localtime())
    line = []
    line.append('BC2 Data    {}\n'.format(timestr))
    line.append('Live data directory: /import/bc2/data/databases\n\n')
    line.append('{:<16s}{:<13s}{:<26s}{:<19}{:<60s}\n\n'.format(
        'Target',
        'Status',
        'Next update',
        'Responsable',
        'Email'))
    fo.write(''.join(line))
    # jobs 
    firstline = True
    for job in fs:
        if firstline:
            firstline = False
            continue
        dbname = job.split()[0]
        person = statusdict[dbname.split('-stable')[0]]['person']
        email = statusdict[dbname.split('-stable')[0]]['email']
        nextupdate = ' '.join(job.split()[9:12])[:-1]
        if not dbname.split('-')[-1] == 'stable':
            status = statusdict[dbname]['status']
        else:
            status = 'up_to_date'
        line = '{:<16s}{:<13s}{:<26s}{:<19}{:<60s}\n'.format(
            dbname, status, nextupdate, person, email)
        fo.write(line)
    fo.close()
    fs.close()
    return

def initial_state(data, databases, e, latest, stable, previous, updating, update_stable):

    fail = False

    # Test 1: No *-updating directories
    xdir = os.path.join(data, '{}-{}'.format(e, updating))
    if os.path.exists(xdir):
        print "ERROR: {} exists".format(xdir)
        fail = True
    # Test 2: No *-update-stable directories
    xdir = os.path.join(data, '{}-{}'.format(e, update_stable))
    if os.path.exists(xdir):
        print "ERROR: {} exists".format(xdir)
        fail = True
    # Test 3: No more that 3 directories
    dirpattern = os.path.join(data,'{}-*'.format(e))
    if len(glob.glob(dirpattern)) > 3:
        print "ERROR: More that 3 directories: {}".format(dirpattern)
        fail = True

    # Test 4: Check correct linking
    listing = glob.glob(os.path.join(data,'{}-*'.format(e)))
    listing.sort()
    LATEST = os.path.join(databases, e, latest)
    STABLE = os.path.join(databases, e, stable)
    PREVIOUS = os.path.join(databases, e, previous)

    # No directories. Create initial structure
    if len(listing) == 0:
        ndir = os.path.join(data,'{}-initial'.format(e))
        os.makedirs(ndir)
        os.remove(LATEST)
        os.symlink(ndir, LATEST)
        os.remove(STABLE)
        os.symlink(ndir, STABLE)
        os.remove(PREVIOUS)
        os.symlink(ndir, PREVIOUS)
    # 
    if len(listing) == 1:
        ndir = listing[0]
        os.remove(LATEST)
        os.symlink(ndir, LATEST)
        os.remove(STABLE)
        os.symlink(ndir, STABLE)
        os.remove(PREVIOUS)
        os.symlink(ndir, PREVIOUS)
    #
    if len(listing) == 2:
        # oldest goes to "previous"
        ndir = listing[0]
        try:
            os.remove(PREVIOUS)
        except:
            pass
        os.symlink(ndir, PREVIOUS)
        # newest goes to "latest"
        ndir = listing[1]
        try:
            os.remove(LATEST)
        except:
            pass
        os.symlink(ndir, LATEST)
        # check that "stable" points to one of the two, if not assign it to newest
        try:
            sdir = os.readlink(STABLE)
            if not sdir == listing[0] and not sdir == listing[1]:
                os.remove(STABLE)
                os.symlink(ndir, STABLE)
        except:
            os.symlink(ndir, STABLE)
    #
    if len(listing) == 3:
        try:
            os.remove(PREVIOUS)
        except:
            pass
        os.symlink(listing[0], PREVIOUS)
        try:
            os.remove(STABLE)
        except:
            pass
        os.symlink(listing[1], STABLE)
        try:
            os.remove(LATEST)
        except:
            pass
        os.symlink(listing[2], LATEST)

    return fail
 
#######################################################################
#main
if __name__ == "__main__":

    #set up options
    settings = get_settings()
    basepath = settings['basepath']
    plugindir = os.path.join(basepath, settings['plugindir'])
    data = os.path.join(basepath, settings['data'])
    databases = os.path.join(basepath, settings['databases'])
    fldownloaded = settings['markerupdated']
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
    #inital run. registration of plugins
        plugins = update_plugin_list(plugindir)
        runscr = {}
        person = {}
        email = {}
        flagdownloaded = []
        for i, e in enumerate(plugins):
            module = importlib.import_module('plugins.{}'.format(e))
            update_daily = getattr(module, 'check_update_daily')
            update_stable = getattr(module, 'check_update_stable')
            runscr[e] = getattr(module, 'run')
            second = getattr(module, 'second')
            minute = getattr(module, 'minute')
            hour = getattr(module, 'hour')
            doweek = getattr(module, 'day_of_week')
            stable_second = getattr(module, 'stable_second')
            stable_minute = getattr(module, 'stable_minute')
            stable_hour = getattr(module, 'stable_hour')
            stable_doweek = getattr(module, 'stable_day_of_week')
            person[e] = getattr(module, 'person')
            email[e] = getattr(module, 'email')
            datadbupdate = os.path.join(data, '{}-updating'.format(e))
            
            # check start up state
            fail = initial_state(data, databases, e, 'latest', 'stable', 'previous',
                'updating', 'update-stable')
            if fail:
                raise Exception, 'Unclean inital state'

            # register jobs (daily and stable)
            LATEST = os.path.join(databases, e, 'latest')
            udir = os.path.join(data, '{}-updating'.format(e))
            arguments = [udir, LATEST, flupdate]
            scheduler.add_job(update_daily, 'cron', args = arguments, name = e,
                day_of_week = doweek, hour = hour, minute = minute, second = second)
            usdir = os.path.join(data, '{}-update-stable'.format(e))
            arguments = [usdir, LATEST, flupdatestable]
            scheduler.add_job(update_stable, 'cron', args = arguments, name = '{}-stable'.format(e),
                day_of_week = stable_doweek, hour = stable_hour, minute = stable_minute, second = stable_second)

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
                udir = os.path.join(data, '{}-updating'.format(e))

                # there is a db to update
                updateme = os.path.join(udir, flupdate)
                if os.path.isfile(updateme):
                    shutil.rmtree(udir)
                    update_latest(runscr[e], data, databases, e, 'latest', 'stable', 'previous', 'updating', fldownloaded, flwontupdate)
                    dlstatus = 'updating'

                # there is not db to update
                dont_updateme = os.path.join(udir, flwontupdate)
                if os.path.isfile(dont_updateme):
                    shutil.rmtree(udir)

                # is there a db updating?
                if os.path.exists(udir):
                    dlstatus = 'updating'

                # finished downloading. mv directories, update symlink.
                downloaded = os.path.join(udir, fldownloaded)
                if os.path.isfile(downloaded):
                    os.remove(downloaded)
                    timestr = time.strftime("%H:%M:%S", time.localtime())
                    ldir = os.readlink(os.path.join(databases, e, 'latest'))
                    sdir = os.readlink(os.path.join(databases, e, 'stable'))
                    pdir = os.readlink(os.path.join(databases, e, 'previous'))
                    ndir = os.path.join(data, '{}-{}'.format(e, timestr))
                    shutil.move(udir, ndir)
                    #are there other symlink pointing to LATEST?
                    if ldir == sdir or ldir == pdir:
                        os.remove(LATEST)
                        os.symlink(ndir, LATEST)
                    else:
                        shutil.rmtree(ldir)
                        os.remove(LATEST)
                        os.symlink(ndir, LATEST)

                # update stable if there is not daily update running
                usdir = os.path.join(data, '{}-update-stable'.format(e))
                LATEST = os.path.join(databases, e, 'latest')
                STABLE = os.path.join(databases, e, 'stable')
                PREVIOUS = os.path.join(databases, e, 'previous')
                ldir = os.readlink(LATEST)
                sdir = os.readlink(STABLE)
                pdir = os.readlink(PREVIOUS)
                if os.path.exists(usdir) and not os.path.exists(udir):
                        shutil.rmtree(usdir)
                        os.remove(PREVIOUS)
                        shutil.rmtree(pdir)
                        os.symlink(sdir, PREVIOUS)
                        os.remove(STABLE)
                        os.symlink(ldir, STABLE)

                status[e] = dict(status = dlstatus, person = person[e], email = email[e])

            update_status(status, 'status.log', 'schedulerjobs.log')
            
    except (KeyboardInterrupt, SystemExit):
         scheduler.shutdown()
#        logger.info('Cancelled')

