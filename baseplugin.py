import logging
import os
import datetime
import shutil
import threading
import glob
import errno


# class Base(object):
class Base:

    def __init__(self):
        self.method = 'scratch'
        self.contact = ''
        self.email = ''
        self.dep = {}
        self.UPDATE_STABLE = False

    def init(self, name, store='INIT_ME', links='INIT_ME'):
        self.__name__ = name
        self.logger = logging.getLogger(self.__name__)

        #inital checks
        if self.method != 'scratch' and self.method != 'incremental':
            raise Exception('Update method not recognized')

        # runtime constants
        self.SGN_FINISHED = 'update_finished'
        self.SGN_UPDATING = 'updating'
        self.SGN_UPTODATE = 'up_to_date'
        self.SGN_UPDATEME = 'update_me'
        self.SGN_CHECKING = 'checking'
        self.SGN_FROZEN = 'FROZEN_VERSION'
        self.SGN_LATEST = 'latest'
        self.SGN_STABLE = 'stable'
        self.SGN_PREVIOUS = 'previous'

        # runtime variables
        self.STORE = store
        self.LINKS = links
        self.status = self.SGN_UPTODATE
        self.status_stable = self.SGN_UPTODATE
        self.l_latest = os.path.join(self.LINKS, self.__name__, self.SGN_LATEST)
        self.l_stable = os.path.join(self.LINKS, self.__name__, self.SGN_STABLE)
        self.l_previous = os.path.join(self.LINKS, self.__name__, self.SGN_PREVIOUS)
        self.l_updating = os.path.join(self.STORE, '{}-{}'.format(self.__name__, self.SGN_UPDATING))
        self.d_latest = ''
        self.d_stable = ''
        self.d_previous = ''
        self.d_updating = ''
        self.d_checking = os.path.join(self.STORE, '{}-{}'.format(self.__name__, self.SGN_CHECKING))

    def check_update(self, a, b):
        raise Exception('NotImplemented. '
            'The method needs to be implemented in subclasses')

    def run(self, a, b):
        raise Exception('NotImplemented. '
            'The method needs to be implemented in subclasses')

    def freq(self, sec=None, min=None, hour=None, dow=None):
        if sec is None and min is None and hour is None and dow is None:
            raise Exception("No update frequency provided")
        self.second = sec
        self.minute = min
        self.hour = hour
        self.day_of_week = dow

    def freq_stable(self, sec=None, min=None, hour=None, dow=None):
        self.stable_second = sec
        self.stable_minute = min
        self.stable_hour = hour
        self.stable_day_of_week = dow
        if sec is not None or min is not None or hour is not None or dow is not None:
            self.UPDATE_STABLE = True

    def refreshlinks(self):
        self.d_latest = os.readlink(self.l_latest)
        self.d_stable = os.readlink(self.l_stable)
        self.d_previous = os.readlink(self.l_previous)

    def check(self, TMPPATH=None):
        if TMPPATH is None:
            TMPPATH=self.d_checking
        self.status = self.SGN_CHECKING
        self.logger.info(self.status)
        try:
            os.makedirs(TMPPATH)
        except:
            if not os.path.isdir(TMPPATH):
                raise
            else:
                os.removedirs(TMPPATH)
                os.makedirs(TMPPATH)

        if self.check_update(TMPPATH, self.l_latest):
            self.status = self.SGN_UPDATEME
        else:
            self.status = self.SGN_UPTODATE
        self.logger.info(self.status)
        os.removedirs(TMPPATH)
        return

    def check_update_stable(self):
        self.status_stable = self.SGN_UPDATEME

    def update_db_stable(self):
        if self.status == self.SGN_UPTODATE and self.d_stable != self.d_latest:
            os.remove(self.l_stable)
            os.symlink(self.d_latest , self.l_stable)
            os.remove(self.l_previous)
            os.symlink(self.d_stable, self.l_previous)
            # initial case, "previous" and "stable" are the same
            isfrozen = os.path.isfile(os.path.join(self.d_previous, self.SGN_FROZEN))
            if self.d_stable != self.d_previous and not isfrozen:
                shutil.rmtree(self.d_previous)
            self.refreshlinks()
            self.status_stable = self.SGN_UPTODATE

    def update_db(self, wait=False):
        ''' create new directory and launch run() function in a new thread '''

        self.status = self.SGN_UPDATING
        timestamp = datetime.datetime.now().strftime('-%y%m%dT%H%M%S')
        self.d_updating = os.path.join(self.STORE, self.__name__ + timestamp)

        if self.method == 'incremental':
            shutil.copytree(self.l_latest, self.d_updating)
        elif self.method == 'scratch':
            os.mkdir(self.d_updating)
        os.symlink(self.d_updating, self.l_updating)
        run_thread = threading.Thread(target=self.run, args=[self.l_updating])
        if not wait:
            run_thread.setDaemon(True)
        run_thread.start()

    def update_links(self):
        self.refreshlinks()
        os.remove(self.l_updating)
        # are there other symlink pointing to LATEST?
        # also, do not delete directory if frozen
        isfrozen = os.path.isfile(os.path.join(self.d_latest, self.SGN_FROZEN))
        if self.d_latest != self.d_stable and self.d_latest != self.d_previous and not isfrozen:
                shutil.rmtree(self.d_latest)
        os.remove(self.l_latest)
        os.symlink(self.d_updating, self.l_latest)
        self.refreshlinks()
        self.status = self.SGN_UPTODATE




    def initial_state_clean(self, settings):
        # Test 1: No *-updating directories
        if os.path.exists(self.l_updating):
            raise Exception('Unclean inital state. '
                '{} exists'.format(self.l_updating))
        if os.path.exists(self.d_checking):
            raise Exception('Unclean inital state. '
                '{} exists'.format(self.d_checking))
    
        # Test 2: No more that 3 not-frozen directories
        pathdirs = os.path.join(self.STORE, self.__name__ + '-*')
        alldirs = glob.glob(pathdirs)
        frozenpath = os.path.join(self.STORE, self.__name__ + '-*', self.SGN_FROZEN)
        frozenflags = glob.glob(frozenpath)
        frozendirs = [f[:-len('/' + self.SGN_FROZEN)] for f in frozenflags]
        listing = list(set(alldirs) - set(frozendirs))
        if len(listing) > 3:
            raise Exception('Unclean inital state. '
                'More than 3 non-frozen versions: {}'.format(pathdirs))

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
   
        # Assign links to directories
        listing.sort()
        makedirs_existsok(os.path.join(self.LINKS, self.__name__))
        # no directories, create initial structure
        if len(listing) == 0:
            ndir = os.path.join(self.STORE, self.__name__ + '-initial')
            makedirs_existsok(ndir)
            remove_nexistok(self.l_latest)
            os.symlink(ndir, self.l_latest)
            remove_nexistok(self.l_stable)
            os.symlink(ndir, self.l_stable)
            remove_nexistok(self.l_previous)
            os.symlink(ndir, self.l_previous)
        if len(listing) == 1:
            remove_nexistok(self.l_latest)
            os.symlink(listing[0], self.l_latest)
            remove_nexistok(self.l_stable)
            os.symlink(listing[0], self.l_stable)
            remove_nexistok(self.l_previous)
            os.symlink(listing[0], self.l_previous)
        if len(listing) == 2:
            # oldest goes to "previous"
            ndir = listing[0]
            remove_nexistok(self.l_previous)
            os.symlink(ndir, self.l_previous)
            # newest goes to "latest"
            ndir = listing[1]
            remove_nexistok(self.l_latest)
            os.symlink(ndir, self.l_latest)
            # "stable" points to anyone, if not, assign it to newest
            try:
                sdir = os.readlink(self.l_stable)
                if not sdir == listing[0] and not sdir == listing[1]:
                    remove_nexistok(self.l_stable)
                    os.symlink(ndir, self.l_stable)
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise
                else:
                    os.symlink(ndir, self.l_stable)
        if len(listing) == 3:
            remove_nexistok(self.l_previous)
            os.symlink(listing[0], self.l_previous)
            remove_nexistok(self.l_stable)
            os.symlink(listing[1], self.l_stable)
            remove_nexistok(self.l_latest)
            os.symlink(listing[2], self.l_latest)
    
        return True
