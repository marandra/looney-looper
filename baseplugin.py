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
        self.set_method()
        self.set_contact()
        self.set_freq()

    def set_method(self, method='scratch'):
         methods = ['scratch', 'incremental', 'dependent']
         if method not in methods:
             raise Exception('Update method not recognized. '\
                 'Currently recognized: {}'.format(methods))
         else:
             self.method = method

    def set_contact(self, contact='None', email='None'):
        self.contact = contact
        self.email = email

    def set_freq(self, sec=None, min=None, hour=None, day=None, dow=None):
        self.s, self.m, self.h, self.d, self.dow = sec, min, hour, day, dow

    def check_freq(self):
        if not any([self.s, self.m, self.h, self.d, self.dow]):
            raise Exception("No update frequency provided")

    def set_functions(self):
         if self.method is 'scratch':
             self.check = self.check_scratch
             self.updatedb = self.updatedb_scratch
         if self.method is 'incremental':
             self.check = self.check_scratch
             self.updatedb = self.updatedb_incremental
         if self.method is ['dependent']:
             self.check = self.check_dependent
             self.updatedb = self.updatedb_dependent

    def set_names(self):
        name = self.__name__
        if self.method is 'dependent':
            if len(name.split('-')) != 2:
             raise Exception('Incorrect format of plugin file name {}'
                 'for "dependent" update method'.format(name))
            dep = name.split('-')[0]
            mod = name.split('-')[1]
        else:
           dep = name
           mod = 'latest'
        return dep, mod

    def init(self, name, store='INIT_ME', links='INIT_ME'):
        self.check_freq()
        self.__name__ = name
        self.logger = logging.getLogger(self.__name__)
        self.set_functions()
        dep, mod = self.set_names()
     
        # runtime constants
        self.FROZEN = 'FROZEN'
        self.STORE = store
        self.LINKS = links
        self.l_mod = os.path.join(self.LINKS, dep, mod)
        self.d_mod = ''
        self.l_updating = os.path.join(self.STORE,
                                       '{}-{}'.format(self.__name__, 'updating'))
        self.d_updating = ''
        self.d_checking = os.path.join(self.STORE,
                                       '{}-{}'.format(self.__name__, 'checking'))
        self.l_prev = os.path.join(self.LINKS, dep, 'prev-' + mod)
        self.d_prev = ''

    def logstate(self, e):
        self.logger.info('Current state: ' + e.dst) 

    def refreshlinks(self, e=None):
        self.d_mod = os.readlink(self.l_mod)
        self.d_prev = os.readlink(self.l_prev)

    def check_scratch(self, e=None):
        TMPPATH = self.d_checking
        os.makedirs(TMPPATH)
        updateavailable = self.check_update(TMPPATH, self.l_mod)
        shutil.rmtree(TMPPATH)
        if updateavailable:
            self.state.doupdate()
        else:
            self.state.nonews()
        return

    def check_dependent(self, e=None):
        if self.state.isstate('up_to_date'):
            self.stablestate.doupdate()
        else:
            self.stablestate.notfinished()
        return

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


    def updatedb_scratch(self, esm):
        ''' create new directory and launch run() function in a new thread '''

        def run(self, path):
            if not self.update(path):
                self.state.finished()
            else:
                self.state.notfinished()

        wait = False
        timestamp = datetime.datetime.now().strftime('-%y%m%dT%H%M%S')
        self.d_updating = os.path.join(self.STORE, self.__name__ + timestamp)

        os.mkdir(self.d_updating)

        os.symlink(self.d_updating, self.l_updating)
        run_thread = threading.Thread(target=self.run, args=[self.l_updating])
        if not wait:
            run_thread.setDaemon(True)
        run_thread.start()


    def updatedb_incremental(self, esm):
        ''' create new directory and launch run() function in a new thread '''

        def run(self, path):
            if not self.update(path):
                self.state.finished()
            else:
                self.state.notfinished()

        wait = False
        timestamp = datetime.datetime.now().strftime('-%y%m%dT%H%M%S')
        self.d_updating = os.path.join(self.STORE, self.__name__ + timestamp)

        self.logger.debug("Copying directory for incremental update")
        shutil.copytree(self.l_mod, self.d_updating)
        try:
            os.remove(os.path.join(self.d_updating, self.FROZEN))
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise

        os.symlink(self.d_updating, self.l_updating)
        run_thread = threading.Thread(target=self.run, args=[self.l_updating])
        if not wait:
            run_thread.setDaemon(True)
        run_thread.start()


    def updatedb_dependent(self, esm):
        ''' create new directory and launch run() function in a new thread '''

        wait = False
        timestamp = datetime.datetime.now().strftime('-%y%m%dT%H%M%S')
        self.d_updating = os.path.join(self.STORE, self.__name__ + timestamp)

        self.logger.debug("Copying directory for incremental update")
        # this can be optimized: rsync, threaded, ...
        shutil.copytree(self.l_mod, self.d_updating)
        try:
            os.remove(os.path.join(self.d_updating, self.FROZEN))
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise

        os.symlink(self.d_updating, self.l_updating)

        self.state.finished()

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
                                  self.FROZEN)
        frozenflags = glob.glob(frozenpath)
        frozendirs = [f[:-len('/' + self.FROZEN)] for f in frozenflags]
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
            remove_nexistok(self.l_prev)
            os.symlink(ndir, self.l_prev)
            remove_nexistok(self.l_mod)
            os.symlink(ndir, self.l_mod)
        if len(listing) == 1:
            remove_nexistok(self.l_mod)
            os.symlink(listing[0], self.l_mod)
            remove_nexistok(self.l_prev)
            os.symlink(listing[0], self.l_prev)
        if len(listing) == 2:
            ndir = listing[0]
            remove_nexistok(self.l_prev)
            os.symlink(ndir, self.l_prev)
            ndir = listing[1]
            remove_nexistok(self.l_mod)
            os.symlink(ndir, self.l_mod)

        self.create_frozen_links()

        return True


    def update_links(self, e):
        self.refreshlinks()
        os.remove(self.l_updating)
        os.remove(self.l_mod)
        os.symlink(self.d_updating, self.l_mod)
        os.remove(self.l_prev)
        os.symlink(self.d_mod, self.l_prev)
        isfrozen = os.path.isfile(os.path.join(self.d_prev, self.FROZEN))
        if self.d_mod != self.d_prev and not isfrozen:
                shutil.rmtree(self.d_prev)
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
                                  self.__name__ + '-*', self.FROZEN)
        frozenflags = glob.glob(frozenpath)
        frozendirs = [f[:-len('/' + self.FROZEN)] for f in frozenflags]
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

