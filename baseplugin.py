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
        self.SGN_LATEST = 'latest'
        self.SGN_STABLE = 'stable'
        self.SGN_PREVIOUS = 'previous'
        self.STORE = store
        self.LINKS = links

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

    def freq(self, second, minute, hour, day_of_week):
        self.second = second
        self.minute = minute
        self.hour = hour
        self.day_of_week = day_of_week

    def freq_stable(self, second, minute, hour, day_of_week):
        self.stable_second = second
        self.stable_minute = minute
        self.stable_hour = hour
        self.stable_day_of_week = day_of_week

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

