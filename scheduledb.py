#!/usr/bin/env python2.7

import ConfigParser
import logging
from apscheduler.schedulers.background import BackgroundScheduler
import time
import sys
import os
import shutil
import glob
import threading
import imp


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
    }
    try:
        settings['plugindir'] = configparser.get('server', 'plugin_repo_path')
        settings['databases'] = configparser.get('server', 'db_link_path')
        settings['data'] = configparser.get('server', 'db_data_path')
    except:
        raise

    return settings


def update_latest(run, data, databases, dbname, latest, stable, previous,
    timestr, fldownloaded, flwontupdate):
    '''
    create new directory and pass it to download function in a new thread
    '''

    ldir = os.readlink(os.path.join(databases, dbname, latest))
    ndir = os.path.join(data, '{}-{}'.format(dbname, timestr))
    # copytree vs mkdir: depending on the strategy, it may be more
    # efficient to start the update from the last version of the database.
    # For our actual cases is better to start with a clean directory.
    # shutil.copytree(ldir, ndir)
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
    line.append('{:<16s}{:<13s}{:<27s}{:<s}\n\n'.format(
        'Target', 'Status', 'Next check', 'Contact'))
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
        line = '{:<16s}{:<13s}{:<27s}{:<s}\n'.format(
            dbname, status, nextupdate, person + ' (' + email + ')')
        fo.write(line)
    fo.close()
    fs.close()
    return


def initial_state(data, databases, e, latest, stable, previous,
                  updating, update_stable, flfrozen):
    '''
    Checks that there is a clean and consistent initial state.
    Function returns 'True' when a test fails.
    Tests: 
      Test 1: No *-updating directories
      Test 2: No *-update-stable directories
      Test 3: No more that 3 not frozen directories
    After passing the test, it assigns the links to the present directories
    '''

    # Test 1: No *-updating directories
    xdir = os.path.join(data, '{}-{}'.format(e, updating))
    if os.path.exists(xdir):
        logger.error("{} exists".format(xdir))
        return True
    # Test 2: No *-update-stable directories
    xdir = os.path.join(data, '{}-{}'.format(e, update_stable))
    if os.path.exists(xdir):
        logger.error("{} exists".format(xdir))
        return True
    # Test 3: No more that 3 not frozen directories
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
    LATEST = os.path.join(databases, e, latest)
    STABLE = os.path.join(databases, e, stable)
    PREVIOUS = os.path.join(databases, e, previous)
    # no directories, create initial structure
    if len(listing) == 0:
        ndir = os.path.join(data, '{}-initial'.format(e))
        # what happens if there an 'initial' dir already?
        os.makedirs(ndir)
        try:
            os.remove(LATEST)
        # here catch only (existing dir) exception
        except:
            pass
        try:
            os.makedirs(os.path.join(databases, e))
        # here catch only (existing dir) exception
        except:
            pass
        os.symlink(ndir, LATEST)
        try:
            os.remove(STABLE)
        except:
            pass
        os.symlink(ndir, STABLE)
        try:
            os.remove(PREVIOUS)
        except:
            pass
        os.symlink(ndir, PREVIOUS)
    if len(listing) == 1:
        try:
            os.makedirs(os.path.join(databases, e))
        # here catch only (existing dir) exception
        except:
            pass
        try:
            os.remove(LATEST)
        except:
            pass
        os.symlink(listing[0], LATEST)
        try:
            os.remove(STABLE)
        except:
            pass
        os.symlink(listing[0], STABLE)
        try:
            os.remove(PREVIOUS)
        except:
            pass
        os.symlink(listing[0], PREVIOUS)
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
        # "stable" points to anyone, if not, assign it to newest
        try:
            sdir = os.readlink(STABLE)
            if not sdir == listing[0] and not sdir == listing[1]:
                os.remove(STABLE)
                os.symlink(ndir, STABLE)
        except:
            os.symlink(ndir, STABLE)
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

    return False


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

    #sys.path.append(databases)

    try:
        # inital run. registration of plugins
        logger.info('Started')
        plugins = map(os.path.basename, glob.glob(os.path.join(plugindir, '*.py')))
        plugins = [p[:-3] for p in plugins]
       
        runscr = {}
        person = {}
        email = {}
        flagdownloaded = []
        for i, e in enumerate(plugins):
            logger.info('Loading plugins: {}'.format(e))
            module = imp.load_source(e, os.path.join(plugindir, e + '.py'))
            instance = module.create()
            runscr[e] = instance.run
            person[e] = instance.person
            email[e] = instance.email

            # check start up state
            fail = initial_state(data, databases, e, 'latest', 'stable',
                 'previous', 'updating', 'update-stable', flfrozen)
            if fail:
                raise Exception('Unclean inital state')

            # register jobs (daily and stable)
            LATEST = os.path.join(databases, e, 'latest')
            cudir = os.path.join(data, '{}-check_update'.format(e))
            arguments = [cudir, LATEST, flupdate, flwontupdate]
            scheduler.add_job(
                instance.check_update_daily, 'cron', args=arguments, name=e,
                day_of_week=instance.day_of_week, hour=instance.hour,
                minute=instance.minute, second=instance.second)
            cusdir = os.path.join(data, '{}-check_update_stable'.format(e))
            arguments = [cusdir, flupdatestable]
            scheduler.add_job(
                instance.check_update_stable, 'cron', args=arguments, name='{}-stable'.format(e),
                day_of_week=instance.stable_day_of_week, hour=instance.stable_hour,
                minute=instance.stable_minute, second=instance.stable_second)

        scheduler.start()

        while True:
            time.sleep(10)
            fo = open('schedulerjobs.log', 'w')
            scheduler.print_jobs(out=fo)
            fo.close()
            status = {}
            for i, e in enumerate(plugins):

                dlstatus = 'up_to_date'
                cudir = os.path.join(data, '{}-check_update'.format(e))
                UPDATING = os.path.join(data, '{}-updating'.format(e))

                # there is a db to update
                updateme = os.path.join(cudir, flupdate)
                if os.path.isfile(updateme) and not os.path.exists(UPDATING):
                    tstamp = time.strftime("%y%m%d-%H:%M:%S", time.localtime())
                    ndir = update_latest(
                        runscr[e], data, databases, e, 'latest', 'stable',
                        'previous', tstamp, fldownloaded, flwontupdate)
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
                    LATEST = os.path.join(databases, e, 'latest')
                    STABLE = os.path.join(databases, e, 'stable')
                    PREVIOUS = os.path.join(databases, e, 'previous')
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

                # update stable if there is not daily update running
                cusdir = os.path.join(data, '{}-check_update_stable'.format(e))
                if os.path.exists(cusdir) and not os.path.exists(UPDATING):
                    # update paths and directories
                    LATEST = os.path.join(databases, e, 'latest')
                    STABLE = os.path.join(databases, e, 'stable')
                    PREVIOUS = os.path.join(databases, e, 'previous')
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

                status[e] = dict(
                    status=dlstatus, person=person[e], email=email[e])

            update_status(status, 'status.log', 'schedulerjobs.log')

    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info('Cancelled')
