import logging
import os
import datetime
import shutil
import threading


# class Base(object):
class Base:

    def __init__(self):
        self.method = 'scratch'
        self.contact = ''
        self.email = ''
        self.dep = {}

    def init(self, name, store='INIT_ME', links='INIT_ME'):
        self.__name__ = name
        self.logger = logging.getLogger(self.__name__)

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
        self.STORE = store
        self.LINKS = links
        self.UPDATE_STABLE = False

        # runtime variables
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

    def refreshlinks(self):
        self.d_latest = os.readlink(self.l_latest)
        self.d_stable = os.readlink(self.l_stable)
        self.d_previous = os.readlink(self.l_previous)

    def freq(self, sec=None, min=None, hour=None, dow=None):
        self.second = sec
        self.minute = min
        self.hour = hour
        self.day_of_week = dow

    def freq_stable(self, sec=None, min=None, hour=None, dow=None):
        if sec is None and min is None and hour is None and dow is None:
            self.UPDATE_STABLE = False
        else:
            self.UPDATE_STABLE = True
        self.stable_second = sec
        self.stable_minute = min
        self.stable_hour = hour
        self.stable_day_of_week = dow

    def update_stable(self):
        self.status_stable = self.SGN_UPDATEME
        self.logger.debug(' (stable) ' + self.status_stable)
        return

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

    def check_update(self, a, b):
        raise Exception('NotImplemented. '
            'The method needs to be implemented in subclasses')

    def run(self, a, b):
        raise Exception('NotImplemented. '
            'The method needs to be implemented in subclasses')

    def update(self, wait=False):
        ''' create new directory and launch run() function in a new thread '''

        self.status = self.SGN_UPDATING
        timestamp = datetime.datetime.now().strftime('-%y%m%dT%H%M%S')
        self.d_updating = os.path.join(self.STORE, self.__name__ + timestamp)

        if self.method == 'incremental':
            shutil.copytree(self.l_latest, self.d_updating)
        else:
            os.mkdir(self.d_updating)
        os.symlink(self.d_updating, self.l_updating)
        run_thread = threading.Thread(target=self.run, args=[self.l_updating])
        if not wait:
            run_thread.setDaemon(True)
        run_thread.start()

    def initial_state_not_clean(self, p, settings):
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

        # Test 1: No *-updating directories
        if self.l_updating:
            raise Exception(self.l_updating + " exists")
        if self.d_checking:
            raise Exception(self.d_checking + " exists")
    
    
        # Test 2: No more that 3 not-frozen directories
        pathdirs = os.path.join(self.STORE, p.__name__ + '-*')
        alldirs = glob.glob(pathdirs)
        frozenpath = os.path.join(self.STORE, p.__name__ + '-*', p.SGN_FROZEN)
        frozenflags = glob.glob(frozenpath)
        frozendirs = [f[:-len('/' + p.SGN_FROZEN)] for f in frozenflags]
        listing = list(set(alldirs) - set(frozendirs))
        if len(listing) > 1:
            raise Exception("More than 1 non-frozen version: {}".format(pathdirs))
    
    
        # Assign links to directories
        listing.sort()
        makedirs_existsok(os.path.join(links, p.__name__))
        # no directories, create initial structure
        if len(listing) == 0:
            ndir = os.path.join(store, '{}-initial'.format(e))
            makedirs_existsok(ndir)
            remove_nexistok(p.l_latest)
            os.symlink(ndir, p.l_latest)
            remove_nexistok(p.l_stable)
            os.symlink(ndir, p.l_stable)
            remove_nexistok(p.l_previous)
            os.symlink(ndir, p.l_previous)
        if len(listing) == 1:
            remove_nexistok(p.l_latest)
            os.symlink(listing[0], p.l_latest)
            remove_nexistok(p.l_stable)
            os.symlink(listing[0], p.l_stable)
            remove_nexistok(p.l_previous)
            os.symlink(listing[0], p.l_previous)
        if len(listing) == 2:
            # oldest goes to "previous"
            ndir = listing[0]
            remove_nexistok(p.l_previous)
            os.symlink(ndir, p.l_previous)
            # newest goes to "latest"
            ndir = listing[1]
            remove_nexistok(p.l_latest)
            os.symlink(ndir, p.l_latest)
            # "stable" points to anyone, if not, assign it to newest
    
            try:
                sdir = os.readlink(p.l_stable)
                if not sdir == listing[0] and not sdir == listing[1]:
                    remove_nexistok(p.l_stable)
                    os.symlink(ndir, p.l_stable)
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise
                else:
                    os.symlink(ndir, p.l_stable)
        if len(listing) == 3:
            remove_nexistok(p.l_previous)
            os.symlink(listing[0], p.l_previous)
            remove_nexistok(p.l_stable)
            os.symlink(listing[1], p.l_stable)
            remove_nexistok(p.l_latest)
            os.symlink(listing[2], p.l_latest)
    
    
        return False

