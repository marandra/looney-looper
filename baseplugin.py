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
         if self.method is 'dependent':
             self.check = self.check_dependent
             self.updatedb = self.updatedb_dependent

    def set_pathnames(self):
        name = self.__name__
        if self.method is 'dependent':
            if len(name.split('-')) != 2:
             raise Exception('Incorrect format of plugin file name {}'
                 'for "dependent" update method'.format(name))
            self.dep = name.split('-')[0]
            self.mod = name.split('-')[1]
        else:
           self.dep = name
           self.mod = 'latest'
        return

    def initial_state_clean(self):
        # Test 1: No updating or frozen links
        if os.path.exists(self.l_updating):
            raise Exception('Unclean inital state. '
                            '{} exists'.format(self.l_updating))
        if os.path.exists(self.d_checking):
            raise Exception('Unclean inital state. '
                            '{} exists'.format(self.d_checking))

        # Test 2: No more that 2 not-frozen directories
        pathdirs = os.path.join(self.STORE, self.__name__ + '_*')
        alldirs = glob.glob(pathdirs)
        frozenpath = os.path.join(self.STORE, self.__name__ + '_*',
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
            ndir = os.path.join(self.STORE, self.dep + '_000000T000000')
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

    def refreshlinks(self, e=None):
        self.d_mod = os.readlink(self.l_mod)
        self.d_prev = os.readlink(self.l_prev)

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

    def init(self, name, store='INIT_ME', links='INIT_ME'):
        self.check_freq()
        self.__name__ = name
        self.logger = logging.getLogger(self.__name__)
        self.set_functions()
        self.set_pathnames()
     
        self.FROZEN = 'FROZEN'
        self.STORE = store
        self.LINKS = links
        self.l_mod = os.path.join(self.LINKS, self.dep, self.mod)
        self.d_mod = ''
        self.l_updating = os.path.join(self.STORE,
                                       '{}-{}'.format(self.__name__, 'updating'))
        self.d_updating = ''
        self.d_checking = os.path.join(self.STORE,
                                       '{}-{}'.format(self.__name__, 'checking'))
        self.l_prev = os.path.join(self.LINKS, self.dep, 'prev-' + self.mod)
        self.d_prev = ''
     
        # check start up state
        try:
            self.initial_state_clean()
        except:
            raise

        self.refreshlinks()

    def logstate(self, e):
        self.logger.info('Current state: ' + e.dst) 

    def _timestamp(self):
        return datetime.datetime.now().strftime('_%y%m%dT%H%M%S')

    def status(self):
        with open('schedulerjobs.log', 'r') as fs:
            for job in fs:
                if self.__name__ == job.split()[0]:
                    nextupdate = ' '.join(job.split()[10:12])
                    break
        line = '{:<21s}{:<13s}{:<27s}{:<s}'.format(
            '{}/{}'.format(self.dep, self.mod),
            self.state.current, nextupdate,
            '{} ({})'.format(self.contact, self.email))
        return line

    def check_scratch(self, e):
        p = e.args[0]['plugins']
        TMPPATH = self.d_checking
        os.makedirs(TMPPATH)
        updateavailable = self.check_update(TMPPATH, self.l_mod)
        shutil.rmtree(TMPPATH)
        if updateavailable:
            self.state.doupdate({'plugins': p})
        else:
            self.state.nonews()
        return

    def check_dependent(self, e):
        p = e.args[0]['plugins']
        if not self.dep in p:
            raise Exception('{} plugin not present'.format(self.dep))
        
        if p[self.dep].state.isstate('up_to_date') \
            and p[self.dep].d_mod != self.d_mod:
                self.state.doupdate({'plugins': p})
        else:
            self.state.nonews()
        return


    def updatedb_scratch(self, e):
        ''' create new directory and launch run() function in a new thread '''

        def run(self, path):
            if not self.update(path):
                self.state.finished({'plugins': p})
            else:
                self.state.notfinished()

        p = e.args[0]['plugins']
        wait = False
        self.d_updating = os.path.join(self.STORE, '{}{}'.format(self.__name__, self._timestamp()))

        os.mkdir(self.d_updating)

        os.symlink(self.d_updating, self.l_updating)
        run_thread = threading.Thread(target=run, args=[self, self.l_updating])
        if not wait:
            run_thread.setDaemon(True)
        run_thread.start()

    def updatedb_incremental(self, e):
        ''' create new directory and launch run() function in a new thread '''

        def run(self, path):
            if not self.update(path):
                self.state.finished({'plugins': p})
            else:
                self.state.notfinished()

        p = e.args[0]['plugins']
        wait = False
        self.d_updating = os.path.join(self.STORE, '{}{}'.format(self.__name__, self._timestamp()))

        self.logger.debug("Copying directory for incremental update")
        shutil.copytree(self.l_mod, self.d_updating)
        try:
            os.remove(os.path.join(self.d_updating, self.FROZEN))
        except OSError as err:
            if err.errno != errno.ENOENT:
                raise

        os.symlink(self.d_updating, self.l_updating)
        run_thread = threading.Thread(target=run, args=[self, self.l_updating])
        if not wait:
            run_thread.setDaemon(True)
        run_thread.start()


    def updatedb_dependent(self, e):
        p = e.args[0]['plugins']
        self.d_updating = p[self.dep].d_mod
        os.symlink(self.d_updating, self.l_updating)
        self.state.finished({'plugins': p})


    def update_links(self, e):
        p = e.args[0]['plugins']
        self.refreshlinks()
        os.remove(self.l_updating)
        os.remove(self.l_mod)
        os.symlink(self.d_updating, self.l_mod)
        os.remove(self.l_prev)
        os.symlink(self.d_mod, self.l_prev)
        frozen = os.path.isfile(os.path.join(self.d_prev, self.FROZEN))
        clear = True
        for key in p:
            if self.d_prev == p[key].d_prev:
                clear = False
        if self.d_mod != self.d_prev and not frozen is clear:
                shutil.rmtree(self.d_prev)
        self.refreshlinks()

        # TODO: check that the following is ok:
        self.create_frozen_links()


    def check_update(self, a, b):
        raise Exception('NotImplemented. '
                        'The method needs to be implemented in subclasses')

    def update(self, path):
        raise Exception('NotImplemented. '
                        'The method needs to be implemented in subclasses')

