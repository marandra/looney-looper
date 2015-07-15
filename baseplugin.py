import logging
import os
import datetime
import shutil
import threading
import glob
import errno
import transitions
import time

# class Base(object):
class Base:

    def __init__(self):
        self.method = 'scratch'
        self.contact = ''
        self.email = ''
        self.dep = {}
        self.UPDATE_STABLE = False

    def logstate(self, e):
        self.logger.info('Current state: ' + e.dst) 

    def init(self, name, store='INIT_ME', links='INIT_ME'):
        self.__name__ = name
        self.logger = logging.getLogger(self.__name__)

        # inital checks
        if self.method != 'scratch' and self.method != 'incremental':
            raise Exception('Update method not recognized')

        # runtime constants
        self.SGN_FROZEN = 'FROZEN_VERSION'
        self.SGN_LATEST = 'latest'
        #self.SGN_STABLE = 'stable'
        self.SGN_PREVIOUS = 'previous'

        # runtime variables
        self.STORE = store
        self.LINKS = links
        self.l_latest = os.path.join(self.LINKS,
                                     self.__name__,
                                     self.SGN_LATEST)
        #self.l_stable = os.path.join(self.LINKS,
        #                             self.__name__,
        #                             self.SGN_STABLE)
        self.l_previous = os.path.join(self.LINKS,
                                       self.__name__,
                                       self.SGN_PREVIOUS)
        self.l_updating = os.path.join(self.STORE,
                                       '{}-{}'.format(self.__name__, 'updating'))
        self.d_latest = ''
        #self.d_stable = ''
        self.d_previous = ''
        self.d_updating = ''
        self.d_checking = os.path.join(self.STORE,
                                       '{}-{}'.format(self.__name__, 'checking'))

    def freq(self, sec=None, min=None, hour=None, day=None, dow=None):
        if not any([sec, min, hour, day, dow]):
            raise Exception("No update frequency provided")
        self.s, self.m, self.h, self.d, self.dow = sec, min, hour, day, dow

    #def freq_stable(self, sec=None, min=None, hour=None, day=None, dow=None):
    #    self.UPDATE_STABLE = any([sec, min, hour, day, dow])
    #    self.ss, self.sm, self.sh, self.sd, self.sdow = sec, min, hour, day, dow

    def refreshlinks(self, e=None):
        self.d_latest = os.readlink(self.l_latest)
        #self.d_stable = os.readlink(self.l_stable)
        self.d_previous = os.readlink(self.l_previous)

    def check(self, e=None):
        TMPPATH = self.d_checking
        os.makedirs(TMPPATH)
        updateavailable = self.check_update(TMPPATH, self.l_latest)
        shutil.rmtree(TMPPATH)
        if updateavailable:
            self.state.doupdate()
        else:
            self.state.nonews()
        return

    #def check_stable(self, e=None):
    #    if self.state.isstate('up_to_date'):
    #        self.stablestate.doupdate()
    #    else:
    #        self.stablestate.notfinished()

    #def update_db_stable(self, e):
    #    if self.d_stable != self.d_latest:
    #        os.remove(self.l_stable)
    #        os.symlink(self.d_latest, self.l_stable)
    #        os.remove(self.l_previous)
    #        os.symlink(self.d_stable, self.l_previous)
    #        # initial case, "previous" and "stable" are the same
    #        isfrozen = os.path.isfile(os.path.join(self.d_previous,
    #                                               self.SGN_FROZEN))
    #        if self.d_stable != self.d_previous and not isfrozen:
    #            shutil.rmtree(self.d_previous)
    #    self.stablestate.finished()

    def run(self, path):
        if not self.update(path):
            self.state.finished()
        else:
            self.state.notfinished()

    def update_db(self, esm):
        ''' create new directory and launch run() function in a new thread '''
        wait = False
        timestamp = datetime.datetime.now().strftime('-%y%m%dT%H%M%S')
        self.d_updating = os.path.join(self.STORE, self.__name__ + timestamp)

        if self.method == 'incremental':
            self.logger.debug("Copying directory for incremental update")
            shutil.copytree(self.l_latest, self.d_updating)
            try:
                os.remove(os.path.join(self.d_updating, self.SGN_FROZEN))
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise
        elif self.method == 'scratch':
            os.mkdir(self.d_updating)

        os.symlink(self.d_updating, self.l_updating)
        run_thread = threading.Thread(target=self.run, args=[self.l_updating])
        if not wait:
            run_thread.setDaemon(True)
        run_thread.start()

    def initial_state_clean(self):
        # Test 1: No updating or frozen links
        if os.path.exists(self.l_updating):
            raise Exception('Unclean inital state. '
                            '{} exists'.format(self.l_updating))
        if os.path.exists(self.d_checking):
            raise Exception('Unclean inital state. '
                            '{} exists'.format(self.d_checking))

        # Test 2: No more that 2 not-frozen directories
        pathdirs = os.path.join(self.STORE, self.__name__ + '-*')
        alldirs = glob.glob(pathdirs)
        frozenpath = os.path.join(self.STORE, self.__name__ + '-*',
                                  self.SGN_FROZEN)
        frozenflags = glob.glob(frozenpath)
        frozendirs = [f[:-len('/' + self.SGN_FROZEN)] for f in frozenflags]
        listing = list(set(alldirs) - set(frozendirs))
        if len(listing) > 2:
            raise Exception('Unclean inital state. '
                            'More than 2 non-frozen versions: '
                            '{}'.format(pathdirs))

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
        listing = alldirs
        listing.sort()
        makedirs_existsok(os.path.join(self.LINKS, self.__name__))
        if len(listing) == 0:
            ndir = os.path.join(self.STORE, self.__name__ + '-000000T000000')
            makedirs_existsok(ndir)
            remove_nexistok(self.l_previous)
            os.symlink(ndir, self.l_previous)
            remove_nexistok(self.l_latest)
            os.symlink(ndir, self.l_latest)
        if len(listing) == 1:
            remove_nexistok(self.l_latest)
            os.symlink(listing[0], self.l_latest)
            remove_nexistok(self.l_previous)
            os.symlink(listing[0], self.l_previous)
        if len(listing) == 2:
            ndir = listing[0]
            remove_nexistok(self.l_previous)
            os.symlink(ndir, self.l_previous)
            ndir = listing[1]
            remove_nexistok(self.l_latest)
            os.symlink(ndir, self.l_latest)

        self.create_frozen_links()

        return True


    def update_links(self, e):
        self.refreshlinks()
        os.remove(self.l_updating)
        os.remove(self.l_latest)
        os.symlink(self.d_updating, self.l_latest)
        os.remove(self.l_previous)
        os.symlink(self.d_latest, self.l_previous)
        isfrozen = os.path.isfile(os.path.join(self.d_previous, self.SGN_FROZEN))
        if self.d_latest != self.d_previous and not isfrozen:
                shutil.rmtree(self.d_previous)
        self.refreshlinks()

        # TODO: check that the following is ok:
        self.create_frozen_links()

#    def initial_state_clean(self):
#        # Test 1: No updating or frozen links
#        if os.path.exists(self.l_updating):
#            raise Exception('Unclean inital state. '
#                            '{} exists'.format(self.l_updating))
#        if os.path.exists(self.d_checking):
#            raise Exception('Unclean inital state. '
#                            '{} exists'.format(self.d_checking))
#
#        # Test 2: No more that 3 not-frozen directories
#        pathdirs = os.path.join(self.STORE, self.__name__ + '-*')
#        alldirs = glob.glob(pathdirs)
#        frozenpath = os.path.join(self.STORE, self.__name__ + '-*',
#                                  self.SGN_FROZEN)
#        frozenflags = glob.glob(frozenpath)
#        frozendirs = [f[:-len('/' + self.SGN_FROZEN)] for f in frozenflags]
#        listing = list(set(alldirs) - set(frozendirs))
#        if len(listing) > 3:
#            raise Exception('Unclean inital state. '
#                            'More than 3 non-frozen versions: '
#                            '{}'.format(pathdirs))
#
#        def makedirs_existsok(path):
#            try:
#                os.makedirs(path)
#            except:
#                if not os.path.isdir(path):
#                    raise
#
#        def remove_nexistok(filename):
#            try:
#                os.remove(filename)
#            except OSError as e:
#                if e.errno != errno.ENOENT:
#                    raise
#
#        # Assign links to directories
#        listing = alldirs
#        listing.sort()
#        makedirs_existsok(os.path.join(self.LINKS, self.__name__))
#        # no directories, create initial structure
#        if len(listing) == 0:
#            ndir = os.path.join(self.STORE, self.__name__ + '-000000T000000')
#            makedirs_existsok(ndir)
#            remove_nexistok(self.l_latest)
#            os.symlink(ndir, self.l_latest)
#            remove_nexistok(self.l_stable)
#            os.symlink(ndir, self.l_stable)
#            remove_nexistok(self.l_previous)
#            os.symlink(ndir, self.l_previous)
#        if len(listing) == 1:
#            remove_nexistok(self.l_latest)
#            os.symlink(listing[0], self.l_latest)
#            remove_nexistok(self.l_stable)
#            os.symlink(listing[0], self.l_stable)
#            remove_nexistok(self.l_previous)
#            os.symlink(listing[0], self.l_previous)
#        if len(listing) == 2:
#            # oldest goes to "previous"
#            ndir = listing[0]
#            remove_nexistok(self.l_previous)
#            os.symlink(ndir, self.l_previous)
#            # newest goes to "latest"
#            ndir = listing[1]
#            remove_nexistok(self.l_latest)
#            os.symlink(ndir, self.l_latest)
#            # "stable" points to anyone, if not, assign it to newest
#            try:
#                sdir = os.readlink(self.l_stable)
#                if not sdir == listing[0] and not sdir == listing[1]:
#                    remove_nexistok(self.l_stable)
#                    os.symlink(ndir, self.l_stable)
#            except OSError as e:
#                if e.errno != errno.ENOENT:
#                    raise
#                else:
#                    os.symlink(ndir, self.l_stable)
#        if len(listing) > 2:
#            remove_nexistok(self.l_previous)
#            os.symlink(listing[-3], self.l_previous)
#            remove_nexistok(self.l_stable)
#            os.symlink(listing[-2], self.l_stable)
#            remove_nexistok(self.l_latest)
#            os.symlink(listing[-1], self.l_latest)
#
#        self.create_frozen_links()
#
#        return True

    def create_frozen_links(self):
        '''create symlinks to frozen versions'''

        def remove_nexistok(filename):
            try:
                os.remove(filename)
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise

        l_frozen = glob.glob(os.path.join(self.LINKS,
                                          self.__name__, 'frozen-*'))
        for lf in l_frozen:
            remove_nexistok(lf)

        frozenpath = os.path.join(self.STORE,
                                  self.__name__ + '-*', self.SGN_FROZEN)
        frozenflags = glob.glob(frozenpath)
        frozendirs = [f[:-len('/' + self.SGN_FROZEN)] for f in frozenflags]
        for fdir in frozendirs:
            l_frozen = os.path.join(self.LINKS, self.__name__,
                                    'frozen-' + fdir.split('-')[-1])
            remove_nexistok(l_frozen)
            os.symlink(fdir, l_frozen)
        return True

    def check_update(self, a, b):
        raise Exception('NotImplemented. '
                        'The method needs to be implemented in subclasses')

    def update(self, path):
        raise Exception('NotImplemented. '
                        'The method needs to be implemented in subclasses')

