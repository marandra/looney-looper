#!/bin/env python
from future import standard_library
standard_library.install_aliases()
from builtins import map
import logging
from apscheduler.schedulers.background import BackgroundScheduler
import configparser
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
import argparse
import pkg_resources


def update_status(status, fname, repo):
    # header
    with open(fname, 'w') as fo:
        timestr = time.strftime("%d %b %Y %H:%M:%S", time.localtime())
        line = []
        line.append('BC2 Data    {}\n'.format(timestr))
        line.append('Live data directory: '
                    '{}\n\n'.format(os.path.abspath(repo)))
        line.append('{:<21s}{:<13s}{:<27s}{:<s}\n\n'.format(
            'Target', 'Status', 'Next check', 'Contact'))
        fo.write(''.join(line))

        for line in status:
            fo.write(line + '\n')


def schedule_plugins(plugins):
    ''' scheduling of jobs '''

    for name, p in list(plugins.items()):
        # register jobs
        scheduler.add_job(
            p.state.checkifupdate, 'cron', name=name,
            args=[{'plugins': plugins}],
            day_of_week=p.dow, hour=p.h, day=p.d, minute=p.m, second=p.s)


def register_plugins(plugindir, store, links):
    ''' registration of plugins and scheduling of jobs '''

    pluginlist = list(map(os.path.basename,
                      glob.glob(os.path.join(plugindir, '*.py'))))
    pluginlist = [p[:-3] for p in pluginlist]

    plugins = {}
    for n in pluginlist:
        logger.info('Found "{}"'.format(n))
        module = imp.load_source(n, os.path.join(plugindir, n + '.py'))
        plugins[n] = module.create()
        plugins[n].init(name=n, store=store, links=links)

    return plugins


def apply_statemachines(plugins):
    ''' states and transitions for state machine '''

    initstate = 'up_to_date'
    events = [
       {'name': 'checkifupdate', 'src': 'up_to_date', 'dst': 'checking'},
       {'name': 'nonews', 'src': 'checking', 'dst': 'up_to_date'},
       {'name': 'doupdate', 'src': 'checking', 'dst': 'updating'},
       {'name': 'doupdate', 'src': 'failed_update', 'dst': 'updating'},
       {'name': 'finished', 'src': 'updating', 'dst': 'up_to_date'},
       {'name': 'notfinished', 'src': 'updating', 'dst': 'failed_update'},
    ]

    for name, p in list(plugins.items()):
        callbacks = {
            'onaftercheckifupdate': p._check,
            'onafterdoupdate': p._update,
            'onbeforefinished': p._update_links,
            'onafterfinished': p._postprocess,
            'onchangestate': p.logstate,
        }
        p.state = fysom.Fysom({'initial': initstate,
                               'events': events,
                               'callbacks': callbacks})


def signal_handling(plugins):
    fnsignal = 'signal'
    try:
        with open(fnsignal, 'r') as f:
            line = f.readline()
        if 'stop' in line:
            os.remove(fnsignal)
            raise Exception("Received 'stop' signal")
        elif 'check' in line.split():
            os.remove(fnsignal)
            pname = line.split()[0]
            if pname in plugins:
                logger.info('Signal-triggered "{}" checking'.format(pname))
                if plugins[pname].state.can('checkifupdate'):
                    plugins[pname].state.checkifupdate({'plugins': plugins})
        else:
            return
    except IOError as e:
        if e.errno == 2:
            pass
        else:
            raise
    return


#######################################################################
# set up global logging and scheduler
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
scheduler = BackgroundScheduler()


def main():
    # get conf file from args
    configfile = pkg_resources.resource_filename("scheduledb",
                                                 "scheduledb.ini")
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--conf', required=False,
                        help='config file location')
    args = parser.parse_args()
    if args.conf is not None:
        configfile = args.conf
        logger.info('Reading configuration file: '
                    '{}'.format(configfile))
    else:
        logger.info('Reading default configuration file: '
                    '{}'.format(configfile))

    # read and set up paths
    config = configparser.ConfigParser()
    config.read(configfile)
    plugindir = config.get('paths', 'plugins')
    store = config.get('paths', 'store')
    links = config.get('paths', 'repository')
    logger.debug('Paths from config file:\n'
                 '  repository: {}\n'
                 '  store: {}\n'
                 '  plugindir: {}'.format(links, store, plugindir))

    # set up options
    refreshtime = int(config.get('advanced', 'refreshtime'))
    logger.debug('Refresh time: {} seconds'.format(refreshtime))

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
            statuslist = []

            for name, p in list(plugins.items()):

                # this state is due to a failed update, let's try again
                if p.state.isstate('failed_update'):
                    p.logger.info('Retrying update')
                    p.state.doupdate({'plugins': plugins})

                statuslist.append(p.status())

            statuslist.sort()
            update_status(statuslist, 'status.log', links)

            signal_handling(plugins)

    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info('Cancelled')

#######################################################################
if __name__ == "__main__":
    main()