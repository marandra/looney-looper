import logging
import os
import datetime
import shutil
import threading
import glob
import errno
import time

# class Base(object):
class Base:

    def __init__(self):
        self.set_method()
        self.set_contact()
        self.set_freq()
        self.set_previous()

    def set_method(self, method='scratch'):
         methods = ['scratch', 'incremental', 'dependent']
         if method not in methods:
             raise Exception('Update method not recognized. '\
                 'Currently recognized: {}'.format(methods))
         else:
             self.method = method

    def set_previous(self, flag=False):
         self.previous = False
         if flag:
             self.previous = True

    def set_contact(self, name='None', email='None'):
        self.contact = name
        self.email = email

    def set_freq(self, sec=None, min=None, hour=None, day=None, dow=None):
        self.s, self.m, self.h, self.d, self.dow = sec, min, hour, day, dow

    def _check_freq(self):
        if not any([self.s, self.m, self.h, self.d, self.dow]):
            raise Exception("No update frequency provided")

    def _set_functions(self):
         if self.method is 'scratch':
             self._check = self._check_scratch
             self._update = self._update_scratch
         if self.method is 'incremental':
             self._check = self._check_scratch
             self._update = self._update_incremental
         if self.method is 'dependent':
             self._check = self._check_dependent
             self._update = self._update_dependent

    def _set_pathnames(self):
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

    def _initial_state_clean(self):

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

        def init_symlinks (self, sl, listing):
            ndir = os.path.join(self.LINKS, self.dep)
            makedirs_existsok(ndir)
            try:
                os.readlink(sl)
            except OSError as e:
                if e.errno == errno.ENOENT:
                    os.symlink('dummy', sl)
            finally:
                if os.readlink(sl) not in listing:
                    ndir = os.path.join(self.STORE, self.dep + '_000000T000000')
                    makedirs_existsok(ndir)
                    remove_nexistok(sl)
                    os.symlink(ndir, sl)

        # Create frozen symlinks
        self._create_frozen_links()
        # No updating or checking links
        if os.path.exists(self.d_checking):
            raise Exception('Unclean inital state. '
                            'Checking directory exists')
        if os.path.exists(self.d_updating):
            raise Exception('Unclean inital state. '
                            'Updating directory exists. '
                            'If this is a continuation of an interrupted update, '
                            'rename "updating" to "updating-cont".')
        # Initialize missing symlinks
        alldirs = glob.glob(os.path.join(self.STORE, self.dep + '_*'))
        listing = list(set(alldirs) - set(self._d_frozen()))
        init_symlinks(self, self.l_mod, listing)
        if self.previous:
            init_symlinks(self, self.l_prev, listing)

        return True

    def _refreshlinks(self, e=None):
        self.d_mod = os.readlink(self.l_mod)
        if self.previous:
            self.d_prev = os.readlink(self.l_prev)

    def _d_frozen(self):
        frozenpath = os.path.join(self.STORE, self.dep + '_*', self.FROZEN)
        frozenflags = glob.glob(frozenpath)
        return [f[:-len('/' + self.FROZEN)] for f in frozenflags]

    def _l_frozen(self):
        return glob.glob(os.path.join(self.LINKS, self.dep, 'frozen_*'))

    def _create_frozen_links(self):
        '''create symlinks to frozen versions'''

        def _remove_nexistok(filename):
            try:
                os.remove(filename)
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise

        for lf in self._l_frozen():
            _remove_nexistok(lf)
        for df in self._d_frozen():
            lf = os.path.join(self.LINKS, self.dep, 'frozen_' + df.split('_')[-1])
            _remove_nexistok(lf)
            os.symlink(df, lf)
        return

    def init(self, name, store='INIT_ME', links='INIT_ME'):
        self._check_freq()
        self.__name__ = name
        self.logger = logging.getLogger(self.__name__)
        self._set_functions()
        self._set_pathnames()
     
        self.FROZEN = 'FROZEN'
        self.STORE = store
        self.LINKS = links
        self.l_mod = os.path.join(self.LINKS, self.dep, self.mod)
        self.d_mod = ''
        self.d_updating = os.path.join(self.STORE,
                                       '{}-{}'.format(self.__name__, 'updating'))
        self.d_checking = os.path.join(self.STORE,
                                       '{}-{}'.format(self.__name__, 'checking'))
        self.l_prev = os.path.join(self.LINKS, self.dep,
                                       '{}-{}'.format(self.mod, 'prev'))
        self.d_prev = ''
     
        # check start up state
        try:
            self._initial_state_clean()
        except:
            raise

        self._refreshlinks()

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

    def _check_scratch(self, e):
        p = e.args[0]['plugins']
        os.makedirs(self.d_checking)
        updateavailable = self.check()
        shutil.rmtree(self.d_checking)
        if updateavailable:
            self.state.doupdate({'plugins': p})
        else:
            self.state.nonews()
        return

    def _check_dependent(self, e):
        p = e.args[0]['plugins']
        if not self.dep in p:
            raise Exception('{} plugin not present'.format(self.dep))
        
        if p[self.dep].state.isstate('up_to_date') \
            and p[self.dep].d_mod != self.d_mod:
                self.state.doupdate({'plugins': p})
        else:
            self.state.nonews()
        return


    def _update_scratch(self, e):
        ''' create new directory and launch run() function in a new thread '''


        def run(self, plugins):
            if not self.update(plugins):
                self.state.finished({'plugins': plugins})
            else:
                self.state.notfinished()

        plugins = e.args[0]['plugins']
        wait = False

        path = self.d_updating
        pathcont = '{}-{}'.format(path, 'cont')
        if os.path.exists(pathcont):
            self.logger.debug("Reusing update directory")
            os.rename(pathcont, path)
        else:
            os.makedirs(path)

        run_thread = threading.Thread(target=run, args=[self, plugins])
        if not wait:
            run_thread.setDaemon(True)
        run_thread.start()

    def _update_incremental(self, e):
        ''' create new directory and launch run() function in a new thread '''

        def run(self):
            if not self.update():
                self.state.finished({'plugins': p})
            else:
                self.state.notfinished()

        p = e.args[0]['plugins']
        wait = False


        path = self.d_updating
        pathcont = '{}-{}'.format(path, 'cont')
        if os.path.exists(pathcont):
            self.logger.debug("Reusing update directory")
            os.rename(pathcont, path)
        else:
            self.logger.debug("Copying directory for incremental update")
            shutil.copytree(self.l_mod, self.d_updating)

        try:
            os.remove(os.path.join(self.d_updating, self.FROZEN))
        except OSError as err:
            if err.errno != errno.ENOENT:
                raise

        run_thread = threading.Thread(target=run, args=[self])
        if not wait:
            run_thread.setDaemon(True)
        run_thread.start()


    def _update_dependent(self, e):
        p = e.args[0]['plugins']
        self.d_updating = p[self.dep].d_mod
        self.state.finished({'plugins': p})


    def _update_links(self, e):
        plugins = e.args[0]['plugins']
        self._refreshlinks()

        os.remove(self.l_mod)
        ndir = os.path.join(self.STORE, '{}{}'.format(self.__name__, self._timestamp()))
        os.rename(self.d_updating, ndir)
        os.symlink(ndir, self.l_mod)

        if self.previous:
            os.remove(self.l_prev)
            os.symlink(self.d_mod, self.l_prev)
            frozen = os.path.isfile(os.path.join(self.d_prev, self.FROZEN))
            clear = True
            for name in plugins:
                if name == self.__name__:
                    continue
                if self.d_prev == plugins[name].d_prev or self.d_prev == plugins[name].d_mod: 
                    clear = False
            if self.d_mod != self.d_prev and not frozen and clear:
                    shutil.rmtree(self.d_prev)
        else:
            frozen = os.path.isfile(os.path.join(self.d_mod, self.FROZEN))
            clear = True
            for name in plugins:
                if name == self.__name__:
                    continue
                if self.d_mod == plugins[name].d_prev or self.d_mod == plugins[name].d_mod: 
                    clear = False
            if not frozen and clear:
                    shutil.rmtree(self.d_mod)

        self._refreshlinks()
        self._create_frozen_links()


    def check(self):
        raise Exception('NotImplemented. '
                        'The method needs to be implemented in subclasses')

    def update(self, plugins):
        raise Exception('NotImplemented. '
                        'The method needs to be implemented in subclasses')

    def _postprocess(self, e):
        plugins = e.args[0]['plugins']
        self.postprocess(plugins)

    def postprocess(self, plugins):
        pass

