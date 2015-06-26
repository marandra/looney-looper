#!/usr/bin/env python2.7

import ConfigParser
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


def get_settings():
    configparser = ConfigParser.SafeConfigParser(os.environ)
    configparser.read("./settings.ini")
    settings = {
        'markerfrozen': "FROZEN_VERSION",
        'log_file': "default.log",
    }
    try:
        settings['plugindir'] = configparser.get('server', 'plugins_path')
        settings['links'] = configparser.get('server', 'db_links_path')
        settings['store'] = configparser.get('server', 'db_store_path')
    except:
        raise

    return settings


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


def initial_state(p, settings):
    '''
    Checks that there is a clean and consistent initial state.
    Function returns 'True' when a test fails.
    Tests: 
      Test 1: No temp (*-updating, update stable, check_update, ...)  directories
      Test 2: No more that 3 not frozen directories
    After passing the test, it assigns the links to the present directories
    '''


    def makedirs_existsok(path):
        try:
            os.makedirs(path)
        except:
            if not os.path.isdir(path):
                raise


    def remove_nexistok(filename):
        try:
            os.remove(filename)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise

    flfrozen = settings['markerfrozen']
    flupdating = p.SGN_UPDATING
    store = settings['store']
    flcheck = p.SGN_CHECKING


    # Test 1: No *-updating directories
    xdir = os.path.join(store, '{}-{}'.format(p.__name__, flupdating))
    if os.path.exists(xdir):
        logger.error("{} exists".format(xdir))
        return True
    xdir = os.path.join(store, '{}-{}'.format(p.__name__, flcheck))
    if os.path.exists(xdir):
        logger.error("{} exists".format(xdir))
        return True


    # Test 2: No more that 3 not-frozen directories
    pathdirs = os.path.join(store, p.__name__ + '-*')
    alldirs = glob.glob(pathdirs)
    frozenpath = os.path.join(store, p.__name__ + '-*', flfrozen)
    frozenflags = glob.glob(frozenpath)
    frozendirs = [f[:-len('/' + flfrozen)] for f in frozenflags]
    listing = list(set(alldirs) - set(frozendirs))
    if len(listing) > 3:
        logger.error("More than 3 not frozen versions: {}".format(pathdirs))
        return True


    # Assign links to directories
    listing.sort()
    makedirs_existsok(os.path.join(links, p.__name__))
    LATEST = p.l_latest
    STABLE = p.l_stable
    PREVIOUS = p.l_previous
    # no directories, create initial structure
    if len(listing) == 0:
        ndir = os.path.join(store, '{}-initial'.format(e))
        makedirs_existsok(ndir)
        remove_nexistok(LATEST)
        os.symlink(ndir, LATEST)
        remove_nexistok(STABLE)
        os.symlink(ndir, STABLE)
        remove_nexistok(PREVIOUS)
        os.symlink(ndir, PREVIOUS)
    if len(listing) == 1:
        remove_nexistok(LATEST)
        os.symlink(listing[0], LATEST)
        remove_nexistok(STABLE)
        os.symlink(listing[0], STABLE)
        remove_nexistok(PREVIOUS)
        os.symlink(listing[0], PREVIOUS)
    if len(listing) == 2:
        # oldest goes to "previous"
        ndir = listing[0]
        remove_nexistok(PREVIOUS)
        os.symlink(ndir, PREVIOUS)
        # newest goes to "latest"
        ndir = listing[1]
        remove_nexistok(LATEST)
        os.symlink(ndir, LATEST)
        # "stable" points to anyone, if not, assign it to newest
        try:
            sdir = os.readlink(STABLE)
            if not sdir == listing[0] and not sdir == listing[1]:
                remove_nexistok(STABLE)
                os.symlink(ndir, STABLE)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise
            else:
                os.symlink(ndir, STABLE)
    if len(listing) == 3:
        remove_nexistok(PREVIOUS)
        os.symlink(listing[0], PREVIOUS)
        remove_nexistok(STABLE)
        os.symlink(listing[1], STABLE)
        remove_nexistok(LATEST)
        os.symlink(listing[2], LATEST)


    return False


def register_plugins(plugindir, settings):
    ''' registration of plugins and scheduling of jobs ''' 

    plugins = map(os.path.basename, glob.glob(os.path.join(plugindir, '*.py')))
    plugins = [p[:-3] for p in plugins]
   
    instance = {}
    for e in plugins:
        logger.info('Loading plugins: {}'.format(e))
        module = imp.load_source(e, os.path.join(plugindir, e + '.py'))
        instance[e] = module.create()
        #instance[e].__name__ = os.path.splitext(os.path.basename(module.__file__))[0]
        instance[e].init(e, store=settings['store'], links=settings['links'])

        # check start up state
        fail = initial_state(instance[e], settings)
        if fail:
            raise Exception('Unclean inital state')

        # register jobs (daily and stable)
        cudir = os.path.join(store, '{}-check_update'.format(e))
        arguments = []
        scheduler.add_job(
            instance[e].check, 'cron', args=arguments, name=e,
            day_of_week=instance[e].day_of_week, hour=instance[e].hour,
            minute=instance[e].minute, second=instance[e].second)
        cusdir = os.path.join(store, '{}-check_update_stable'.format(e))
        arguments = []
        scheduler.add_job(
            instance[e].update_stable, 'cron', args=arguments, name='{}-stable'.format(e),
            day_of_week=instance[e].stable_day_of_week, hour=instance[e].stable_hour,
            minute=instance[e].stable_minute, second=instance[e].stable_second)

    return instance


#######################################################################
# main
if __name__ == "__main__":

    # set up logging and scheduler
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    scheduler = BackgroundScheduler()

    # set up options
    plugindir = get_settings()['plugindir']
    store = get_settings()['store']
    links = get_settings()['links']
    flfrozen = get_settings()['markerfrozen']

    try:
        # initialization. registration of plugins
        logger.info('Started')
        plugins = register_plugins(plugindir, get_settings())
        scheduler.start()

        while True:
            time.sleep(1)
            with open('schedulerjobs.log', 'w') as fo:
                scheduler.print_jobs(out=fo)
            status = {}
            for name, p in plugins.items():

                # there is a db to update
                if p.status == p.SGN_UPDATEME:
                    p.update()

                # finished downloading: rm directory, update symlinks
                if p.status == p.SGN_FINISHED:
                    p.refreshlinks()
                    os.remove(p.l_updating)
                    # are there other symlink pointing to LATEST?
                    # also, do not delete directory if frozen
                    isfrozen = os.path.isfile(os.path.join(p.d_latest, flfrozen))
                    if p.d_latest != p.d_stable and p.d_latest != p.d_previous and not isfrozen:
                            shutil.rmtree(p.d_latest)
                    os.remove(p.l_latest)
                    os.symlink(p.d_updating, p.l_latest)
                    p.refreshlinks()
                    p.status = p.SGN_UPTODATE

                # update stable if there is not daily update running
                if p.status_stable == p.SGN_UPDATEME and p.status == p.SGN_UPTODATE and p.d_stable != p.d_latest:
                    os.remove(p.l_stable)
                    os.symlink(p.d_latest ,ldir, p.l_stable)
                    os.remove(p.l_previous)
                    os.symlink(p.d_stable, p.l_previous)
                    # initial case, "previous" and "stable" are the same
                    isfrozen = os.path.isfile(os.path.join(p.d_previous, flfrozen))
                    if p.d_stable != p.d_previous and not isfrozen:
                        shutil.rmtree(p.d_previous)
                    p.refreshlinks()
                    p.status_stable = p.SGN_UPTODATE

                status[p.__name__] = dict(
                    status=p.status, contact=p.contact, email=p.email)

            update_status(status, 'status.log', 'schedulerjobs.log')

    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info('Cancelled')
