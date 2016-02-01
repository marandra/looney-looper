#!/bin/env python
from future import standard_library
standard_library.install_aliases()
from builtins import map
import logging
from apscheduler.schedulers.background import BackgroundScheduler
import configparser
import time
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

def status_update(plugins, fname, repo):
    '''Generates status file.
       Parameters:

       :plugins: dictionary with plugins
       :fname: file name of the status file
       :repo: location of the database repository

    Generate status file with information about the running plugins,
    its current status, its next scheduled check,
    and the name and email of the contact.
    '''

    col_frmt = '{:<21s}{:<13s}{:<27s}{:<s}'
    date_frmt = '%Y-%m-%d %H:%M:%S'
    timestr = time.strftime(date_frmt, time.localtime())

    header = []
    header.append('BC2 Data    {}'.format(timestr))
    header.append('Live data directory: '
                  '{}\n'.format(os.path.abspath(repo)))
    header.append(col_frmt.format(
        'Target', 'Status', 'Next check', 'Contact'))
    header.append('\n')

    status = []
    for name, p in list(plugins.items()):
        job = scheduler.get_job(name)
        line = col_frmt.format(
            '{}/{}'.format(p.dep, p.mod),
            p.state.current,
            job.next_run_time.strftime(date_frmt),
            '{} ({})'.format(p.contact, p.email))
        status.append(line)
    status.sort()
 
    with open(fname, 'w') as fo:
        fo.write('\n'.join(header))
        fo.write('\n'.join(status))


def schedule_plugins(plugins):
    '''Schedules jobs based on plugins.
       Parameters:

       :plugins: dictionary of the detected plugins

       This routine adds jobs to the scheduler based on the frequency set in
       the plugin.
    '''

    for name, p in list(plugins.items()):
        global scheduler
        scheduler.add_job(
            p.state.checkifupdate, 'cron', name=name, id=name,
            args=[{'plugins': plugins}],
            day_of_week=p.dow, hour=p.h, day=p.d, minute=p.m, second=p.s)


def register_plugins(plugindir, store, links):
    '''Registers detected plugins.
       Parameters:

       :plugindir: location of the plugin directory
       :store: location of the time-tagged database folders
       :links: location of the repository (contining sym links to
               latests version of databases

       This routine scans the plugin directory to find valid plugins
       (any *.py file), generating a dictionary with the detected plugins.
       Each plugin is loaded as a module by the *imp.load_source* method,
       then called the create() and init() methods of the plugin.
    '''

    logger = logging.getLogger(__name__)
    pluginlist = list(map(os.path.basename,
                      glob.glob(os.path.join(plugindir, '*.py'))))
    pluginlist = [p[:-3] for p in pluginlist]
    plugins = {}
    for n in pluginlist:
        logger.info('Found "{}" plugin'.format(n))
        try:
            module = imp.load_source(n, os.path.join(plugindir, n + '.py'))
            plugins[n] = module.create(name=n)
            plugins[n].init(name=n, store=store, links=links)
        except Exception as e:
            logger.exception("Problem loading plugin {}. Skipped.".format(n))
            plugins.pop(n, None)
    return plugins


def apply_statemachines(plugins):
    '''Defines states and transitions for state machine.
       Parameters:

       :plugins: dictionary with plugins

       This routine defines the following posible states for the plugins,
       the events for the transitions, and the actions to take before or
       after entering a new state (see the documentation of Fysom for more
       details).
    '''

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


def signal_handling(plugins, filename):
    '''Reads a text file 'signal' and executes the instruction within.
       Actions implemented:

       :stop: Stops the execution, leaving a clean state for restart.
       :check <pluginname>: Launches the check event for <pluginname>.

       The program will read and execute the recognized instruction on the
       first line of the file, ignoring the rest of the file. The file will
       be deleted afterwards.
    '''
    logger = logging.getLogger(__name__)
    try:
        with open(filename, 'r') as f:
            line = f.readline()
        os.remove(filename)
        if 'stop' in line:
            raise Exception("Received 'stop' signal")
        elif 'check' in line.split():
            pname = line.split()[1]
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


def read_conf_param():
    '''Gets parameters from default and user-provided configuration files.
       Checks the validity of the parameters.
    '''

    options_path = ['plugins', 'store', 'repository', 'logfile',
                   'signalfile', 'statusfile']
    options_advanced = ['refreshtime', 'loglevel']

    def get_params(conffile):
        config = configparser.ConfigParser()
        config.read(conffile)
        section = 'paths'
        if config.has_section(section):
            for option in options_path:
                if config.has_option(section, option):
                    params[option] = config.get(section, option)
        section = 'advanced'
        if config.has_section(section):
            for option in options_advanced:
                if config.has_option(section, option):
                    params[option] = config.get(section, option)
        return params

    def validate_params(params):
        '''insert any necessary check of parameters in this routine.
        '''
        params['loglevel'] = params['loglevel'].upper()
        if params['loglevel'] not in ['INFO', 'DEBUG', 'WARNING',
                                      ' ERROR','CRITICAL']:
            raise Exception('Invalid value for "loglevel" option.')
        params['refreshtime'] = int(params['refreshtime'])
        if params['refreshtime'] < 1:
            raise Exception('Invalid value for "refreshtime" option.')
        return params

    def check_mandatory_params(params):
        for o in options_path + options_advanced:
            if o not in params:
                raise KeyError("Missing parameters in configuration files")
        return

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--conf', required=False,
                        help='config file location')
    args = parser.parse_args()

    usrconffile = args.conf
    dflconffile = pkg_resources.resource_filename('scheduledb',
                                                  'scheduledb.ini')
    params = {}
    params.update(validate_params(get_params(dflconffile)))
    if usrconffile is not None:
        params.update(validate_params(get_params(usrconffile)))
        params['user config'] = usrconffile
    check_mandatory_params(params)
    return params


def setup_custom_logger(name, level, filename):
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-5s '
                                      '%(name)-21s %(message)s')
    handler = logging.FileHandler(filename)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger

#######################################################################

def main():

    # pre-initialization
    param = read_conf_param()
    logger = setup_custom_logger('', param['loglevel'], param['logfile'])
    logger.debug('Params read from configuration file:\n'
                 '    {}'.format(param))
    global scheduler
    scheduler = BackgroundScheduler()

    plugindir = param['plugins']
    store = param['store']
    links = param['repository']

    try:
        # initialization
        logger.info('Scheduledb started')
        plugins = register_plugins(plugindir, store, links)
        machines = apply_statemachines(plugins)
        scheduler.start()
        schedule_plugins(plugins)
        # control loop
        while True:
            time.sleep(int(param['refreshtime']))
            for name, p in list(plugins.items()):
                # this state is due to a failed update, let's try again
                if p.state.isstate('failed_update'):
                    p.logger.info('Retrying update')
                    p.state.doupdate({'plugins': plugins})
            status_update(plugins, param['statusfile'], links)
            signal_handling(plugins, param['signalfile'])

    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info('===== Shut down =====')
        logging.shutdown()
