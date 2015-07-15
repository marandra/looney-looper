#!/usr/bin/env python2.7

import logging
from apscheduler.schedulers.background import BackgroundScheduler
import time
import datetime
import sys
import os
import shutil
import glob
import threading
import imp
import errno
import fysom


def update_status(statusdict, fname, fsched):
    # header
    fo = open(fname, 'w')
    fs = open(fsched, 'r')
    timestr = time.strftime("%d %b %Y %H:%M:%S", time.localtime())
    line = []
    line.append('BC2 Data    {}\n'.format(timestr))
    line.append('Live data directory: /import/bc2/data/test\n\n')
    line.append('{:<21s}{:<13s}{:<27s}{:<s}\n\n'.format(
        'Target', 'Status', 'Next check', 'Contact'))
    fo.write(''.join(line))
    # jobs
    firstline = True
    for job in fs:
        if firstline:
            firstline = False
            continue
        dbname = job.split()[0]
        contact = statusdict[dbname.split('-stable')[0]]['contact']
        email = statusdict[dbname.split('-stable')[0]]['email']
        nextupdate = ' '.join(job.split()[9:12])[:-1]
        if not dbname.split('-')[-1] == 'stable':
            status = statusdict[dbname]['status']
        else:
            status = 'up_to_date'
        line = '{:<21s}{:<13s}{:<27s}{:<s}\n'.format(
            dbname, status, nextupdate, contact + ' (' + email + ')')
        fo.write(line)
    fo.close()
    fs.close()
    return


def schedule_plugins(plugins):
    ''' scheduling of jobs '''

    for name, p in plugins.items():
        # register jobs (daily and stable)
        scheduler.add_job(
            p.state.checkifupdate, 'cron', name=name,
            day_of_week=p.dow, hour=p.h, day=p.d, minute=p.m, second=p.s)
        #if p.UPDATE_STABLE:
        #    scheduler.add_job(
        #        p.stablestate.checkifupdate, 'cron', name='{}-stable'.format(name),
        #        day_of_week=p.sdow, hour=p.sh, day=p.sd, minute=p.sm, second=p.ss)


def register_plugins(plugindir, store, links):
    ''' registration of plugins and scheduling of jobs '''

    plugins = map(os.path.basename, glob.glob(os.path.join(plugindir, '*.py')))
    plugins = [p[:-3] for p in plugins]

    instance = {}
    for e in plugins:
        logger.info('Found "{}"'.format(e))
        module = imp.load_source(e, os.path.join(plugindir, e + '.py'))
        instance[e] = module.create()
        instance[e].init(e, store=store, links=links)

        # check start up state
        try:
            instance[e].initial_state_clean()
        except:
            raise
    return instance


def apply_statemachines(plugins):
    ''' states and trasnitions for state machine '''

    initstate = 'up_to_date'
    events = [
       {'name': 'checkifupdate', 'src': 'up_to_date', 'dst': 'checking'},
       {'name': 'nonews', 'src': 'checking', 'dst': 'up_to_date'},
       {'name': 'doupdate', 'src': 'checking', 'dst': 'updating'},
       {'name': 'doupdate', 'src': 'failed_update', 'dst': 'updating'},
       {'name': 'finished', 'src': 'updating', 'dst': 'up_to_date'},
       {'name': 'notfinished', 'src': 'updating', 'dst': 'failed_update'},
    ]

    for name, p in plugins.items():
        callback = {
            'onaftercheckifupdate': p.check,
            'onafterdoupdate': p.update_db,
            'onbeforefinished': p.update_links,
            'onchangestate': p.logstate,
        }
        p.state = fysom.Fysom({'initial': initstate,
                               'events': events,
                               'callbacks': callback })

    #state machine for stable links
    #initstate = 'up_to_date'
    #events = [
    #   {'name': 'checkifupdate', 'src': 'up_to_date', 'dst': 'checking'},
    #   {'name': 'checkifupdate', 'src': 'failed_update', 'dst': 'checking'},
    #   {'name': 'doupdate', 'src': 'checking', 'dst': 'updating'},
    #   {'name': 'finished', 'src': 'updating', 'dst': 'up_to_date'},
    #   {'name': 'notfinished', 'src': 'checking', 'dst': 'failed_update'},
    #]

    #for name, p in plugins.items():
    #    callback = {
    #        'onaftercheckifupdate': p.check_stable,
    #        'onafterdoupdate': p.update_db_stable,
    #        'onbeforefinished': p.refreshlinks,
    #        'onchangestate': p.logstate,
    #    }
    #    p.stablestate = fysom.Fysom({'initial': initstate,
    #                                 'events': events,
    #                                 'callbacks': callback })
    #  
    #return 


def signal_handling(plugins):
    fnsignal = 'signal'
    try:
        with open(fnsignal, 'r') as f:
            line = f.readline()
        if 'stop' in line:
            os.remove(fnsignal)
            raise Exception("Received 'stop' signal")
        elif 'checknow' in line.split():
            os.remove(fnsignal)
            pname = line.split()[0]
            if pname in plugins:
                logger.info('Signal-triggered "{}" checking'.format(pname))
                plugins[pname].state.checkifupdate()
        else:
            return
    except IOError, e:
        if e.errno == 2:
            pass
        else:
            raise
    return
#######################################################################
# main
if __name__ == "__main__":

    # set up logging and scheduler
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    scheduler = BackgroundScheduler()

    # set up paths
    plugindir = '../.plugins'
    store = '../store'
    links = '..'

    # set up options
    refreshtime = 4

    try:
        # initialization
        logger.info('Started')
        scheduler.start()
        plugins = register_plugins(plugindir, store, links)
        machines = apply_statemachines(plugins)
        schedule_plugins(plugins)

        while True:
            time.sleep(refreshtime)
            with open('schedulerjobs.log', 'w') as fo:
                scheduler.print_jobs(out=fo)
            status = {}

            for name, p in plugins.items():

                # this state should only be after a failed update, let's try again
                if p.state.isstate('failed_update'):
                    p.logger.info('Retrying update')
                    p.state.doupdate()
                #if p.stablestate.isstate('sfailed_update'):
                #    p.logger.info('Retrying update')
                #    p.stablestate.checkifupdate()

                status[p.__name__] = dict(
                    status=p.state.current, contact=p.contact, email=p.email)

            update_status(status, 'status.log', 'schedulerjobs.log')

            signal_handling(plugins)

    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info('Cancelled')
