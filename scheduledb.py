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
        'plugindir': "",
        'databases': "",
        'data': "",
        'markerupdated': "FINISHED_DOWNLOAD",
        'markerwontupdate': "WILL_NOT_UPDATE",
        'markerupdate': "UPDATEME",
        'markerupdatestable': "UPDATEME_STABLE",
        'markerfrozen': "FROZEN_VERSION",
        'log_file': "default.log",
        'latest': "latest",
        'stable': "stable",
        'previous': "previous",
        'sgn_check': "check_update",
        'sgn_checkstable': "check_update_stable",
        'sgn_updating': "updating",
        'sgn_updatingstable': "updating_stable",
    }
    try:
        settings['plugindir'] = configparser.get('server', 'plugin_repo_path')
        settings['databases'] = configparser.get('server', 'db_link_path')
        settings['data'] = configparser.get('server', 'db_data_path')
    except:
        raise

    return settings


def update_latest(name, plugins, settings):
    '''
    create new directory and pass it to download function in a new thread
    '''


    def timestamp():
        ts = datetime.datetime.now().strftime('-%y%m%dT%H%M%S')
        return ts
    
    
    def valid_timedate(plugin, days):
        ''' False if filename is older than 'days' days, or "initial" '''
        filename = plugin.LATEST.split('-')[-1]
        if filename == 'initial' or filename == '':
            return False
        ft = datetime.datetime.strptime(filename, '%y%m%dT%H:%M:%S')
        dt = datetime.date.today() - datetime.timedelta(days=days)
        return ft > dt
    

    run  = plugins[name].run
    dbname = plugins[name].__name__
    method = plugins[name].method

    data = settings['data']
    databases = settings['databases']
    fldownloaded = settings['markerupdated']
    latest = settings['latest']
    stable = settings['stable']
    previous = settings['previous']

    # dependencies
    for pname, days in plugins[name].dep.items():
        if valid_timedate(plugins[pname], days):
           continue
        print "DEBUG UDATING DEP "
        

    ldir = os.readlink(os.path.join(databases, dbname, latest))
    ndir = os.path.join(data, dbname + timestamp())

    if method == 'incremental':
        shutil.copytree(ldir, ndir)
    else:
        os.mkdir(ndir)
    run_thread = threading.Thread(target=run,
                                  args=[ndir, fldownloaded])
    run_thread.start()

    return ndir


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


def initial_state(data, databases, e, settings):
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

    latest = settings['latest']
    stable = settings['stable']
    previous = settings['previous']
    flfrozen = settings['markerfrozen']
    flcheck = settings['sgn_check']
    flupdating = settings['sgn_updating']
    flupdatingstable = settings['sgn_updatingstable']


    # Test 1: No *-updating directories
    xdir = os.path.join(data, '{}-{}'.format(e, flupdating))
    if os.path.exists(xdir):
        logger.error("{} exists".format(xdir))
        return True
    xdir = os.path.join(data, '{}-{}'.format(e, flupdatingstable))
    if os.path.exists(xdir):
        logger.error("{} exists".format(xdir))
        return True
    xdir = os.path.join(data, '{}-{}'.format(e, flcheck))
    if os.path.exists(xdir):
        logger.error("{} exists".format(xdir))
        return True


    # Test 2: No more that 3 not-frozen directories
    pathdirs = os.path.join(data, '{}-*'.format(e))
    alldirs = glob.glob(pathdirs)
    frozenpath = os.path.join(data, '{}-*'.format(e), flfrozen)
    frozenflags = glob.glob(frozenpath)
    frozendirs = [f[:-len('/' + flfrozen)] for f in frozenflags]
    listing = list(set(alldirs) - set(frozendirs))
    if len(listing) > 3:
        logger.error("More than 3 not frozen versions: {}".format(pathdirs))
        return True


    # Assign links to directories
    listing.sort()
    makedirs_existsok(os.path.join(databases, e))
    LATEST = os.path.join(databases, e, latest)
    STABLE = os.path.join(databases, e, stable)
    PREVIOUS = os.path.join(databases, e, previous)
    # no directories, create initial structure
    if len(listing) == 0:
        ndir = os.path.join(data, '{}-initial'.format(e))
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


def regist_plugins(plugindir, settings):
    ''' registration of plugins and scheduling of jobs ''' 

    latest = settings['latest']
    stable = settings['stable']
    previous = settings['previous']

    plugins = map(os.path.basename, glob.glob(os.path.join(plugindir, '*.py')))
    plugins = [p[:-3] for p in plugins]
   
    instance = {}
    for e in plugins:
        logger.info('Loading plugins: {}'.format(e))
        module = imp.load_source(e, os.path.join(plugindir, e + '.py'))
        instance[e] = module.create()
        #instance[e].__name__ = os.path.splitext(os.path.basename(module.__file__))[0]
        instance[e].__name__ = e

        # check start up state
        fail = initial_state(data, databases, e, settings)
        if fail:
            raise Exception('Unclean inital state')

        # register jobs (daily and stable)
        LATEST = os.path.join(databases, e, 'latest')
        cudir = os.path.join(data, '{}-check_update'.format(e))
        arguments = [cudir, LATEST, flupdate, flwontupdate]
        scheduler.add_job(
            instance[e].check_update_daily, 'cron', args=arguments, name=e,
            day_of_week=instance[e].day_of_week, hour=instance[e].hour,
            minute=instance[e].minute, second=instance[e].second)
        cusdir = os.path.join(data, '{}-check_update_stable'.format(e))
        arguments = [cusdir, flupdatestable]
        scheduler.add_job(
            instance[e].check_update_stable, 'cron', args=arguments, name='{}-stable'.format(e),
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
    data = get_settings()['data']
    databases = get_settings()['databases']
    fldownloaded = get_settings()['markerupdated']
    flwontupdate = get_settings()['markerwontupdate']
    flupdate = get_settings()['markerupdate']
    flupdatestable = get_settings()['markerupdatestable']
    flfrozen = get_settings()['markerfrozen']
    sgn_check = get_settings()['sgn_check']
    sgn_checkstable = get_settings()['sgn_checkstable']
    sgn_updating = get_settings()['sgn_updating']
    latest = get_settings()['latest']
    stable = get_settings()['stable']
    previous = get_settings()['previous']

    try:
        # initialization. registration of plugins
        logger.info('Started')
        plugins = regist_plugins(plugindir, get_settings())
        scheduler.start()

        while True:
            time.sleep(2)
            fo = open('schedulerjobs.log', 'w')
            scheduler.print_jobs(out=fo)
            fo.close()
            status = {}
            #for i, e in enumerate(plugins):
            for name, p in plugins.items():

                dlstatus = 'up_to_date'
                cudir = os.path.join(data, '{}-{}'.format(p.__name__, sgn_check))
                UPDATING = os.path.join(data, '{}-{}'.format(p.__name__, sgn_updating))

                # there is a db to update
                updateme = os.path.join(cudir, flupdate)
                if os.path.isfile(updateme) and not os.path.exists(UPDATING):
                    ndir = update_latest(p.__name__, plugins, get_settings())
                    os.symlink(ndir, UPDATING)
                    dlstatus = 'updating'
                try:
                    shutil.rmtree(cudir)
                except:
                    pass

                # there is not db to update
                dont_updateme = os.path.join(cudir, flwontupdate)
                if os.path.isfile(dont_updateme):
                    shutil.rmtree(cudir)

                # is there a db updating?
                try:
                    os.readlink(UPDATING)
                    dlstatus = 'updating'
                except:
                    pass

                # finished downloading. mv directories, update symlinks
                downloaded = os.path.join(UPDATING, fldownloaded)
                if os.path.isfile(downloaded):
                    os.remove(downloaded)
                    # update paths and directories
                    LATEST = os.path.join(databases, p.__name__, latest)
                    STABLE = os.path.join(databases, p.__name__, stable)
                    PREVIOUS = os.path.join(databases, p.__name__, previous)
                    ldir = os.readlink(LATEST)
                    sdir = os.readlink(STABLE)
                    pdir = os.readlink(PREVIOUS)
                    ndir = os.readlink(UPDATING)
                    os.remove(UPDATING)
                    # are there other symlink pointing to LATEST?
                    # also, do not delete directory if frozen
                    isfrozen = os.path.isfile(os.path.join(ldir, flfrozen))
                    if ldir != sdir and ldir != pdir and not isfrozen:
                            shutil.rmtree(ldir)
                    os.remove(LATEST)
                    os.symlink(ndir, LATEST)
                    # update links in instance
                    p.LATEST = ldir
                    p.STABLE = sdir
                    p.PREVIOUS = pdir

                # update stable if there is not daily update running
                cusdir = os.path.join(data, '{}-{}'.format(p.__name__, sgn_checkstable))
                if os.path.exists(cusdir) and not os.path.exists(UPDATING):
                    # update paths and directories
                    LATEST = os.path.join(databases, p.__name__, latest)
                    STABLE = os.path.join(databases, p.__name__, stable)
                    PREVIOUS = os.path.join(databases, p.__name__, previous)
                    ldir = os.readlink(LATEST)
                    sdir = os.readlink(STABLE)
                    pdir = os.readlink(PREVIOUS)
                    shutil.rmtree(cusdir)
                    # only update stable when not up to date
                    if sdir != ldir:
                        os.remove(STABLE)
                        os.symlink(ldir, STABLE)
                        os.remove(PREVIOUS)
                        os.symlink(sdir, PREVIOUS)
                        # initial case, "previous" and "stable" are the same
                        isfrozen = os.path.isfile(os.path.join(pdir, flfrozen))
                        if sdir != pdir and not isfrozen:
                            shutil.rmtree(pdir)
                
                    # update links in instance
                    p.LATEST = ldir
                    p.STABLE = sdir
                    p.PREVIOUS = pdir

                status[p.__name__] = dict(
                    status=dlstatus, contact=p.contact, email=p.email)

            update_status(status, 'status.log', 'schedulerjobs.log')

    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info('Cancelled')
