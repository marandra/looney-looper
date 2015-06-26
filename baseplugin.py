import logging
import os
import datetime
import shutil
import datetime
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
        self.FL_FINISHED = 'FINISHED_DOWNLOAD'
        self.SGN_UPDATING = 'updating'
        self.SGN_UPTODATE = 'up_to_date'
        self.SGN_LATEST = 'latest'
        self.SGN_STABLE = 'stable'
        self.SGN_PREVIOUS = 'previous'
        self.STORE = store
        self.LINKS = links

        # runtime variables
        self.status = self.SGN_UPTODATE
        self.l_latest = os.path.join(self.LINKS, self.__name__, self.SGN_LATEST)
        self.l_stable = os.path.join(self.LINKS, self.__name__, self.SGN_STABLE)
        self.l_previous = os.path.join(self.LINKS, self.__name__, self.SGN_PREVIOUS)
        self.l_updating = os.path.join(self.STORE, '{}-{}'.format(self.__name__, self.SGN_UPDATING))

    def check_freq(self, second, minute, hour, day_of_week):
        self.second = second
        self.minute = minute
        self.hour = hour
        self.day_of_week = day_of_week

    def check_freq_stable(self, second, minute, hour, day_of_week):
        self.stable_second = second
        self.stable_minute = minute
        self.stable_hour = hour
        self.stable_day_of_week = day_of_week

    def check_update_stable(self, PATH, FLAG_UPDATE_STABLE):
        try:
            os.makedirs(PATH)
        except:
            if not os.path.isdir(PATH):
                raise
            else:
                os.remove(PATH)
                os.makedirs(PATH)

        flagpath = os.path.join(PATH, FLAG_UPDATE_STABLE)
        open(flagpath, 'w').close()
        self.logger.debug('Created STABLE {}'.format(flagpath))
        return

    def check_update_daily(self, PATH, LATEST, FLAG_UPDATE, FLAG_WONT_UPDATE):
        try:
            os.makedirs(PATH)
        except:
            if not os.path.isdir(PATH):
                raise
            else:
                os.remove(PATH)
                os.makedirs(PATH)

        UPDATE = self.check_update(PATH, LATEST)
        if UPDATE:
            flagpath = os.path.join(PATH, FLAG_UPDATE)
        else:
            flagpath = os.path.join(PATH, FLAG_WONT_UPDATE)
            self.logger.info('No new updates')
        open(flagpath, 'w').close()
        self.logger.debug('Created DAILY {}'.format(flagpath))
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
        d_new = os.path.join(self.STORE, self.__name__ + timestamp)

        if self.method == 'incremental':
            shutil.copytree(self.l_latest, d_new)
        else:
            os.mkdir(d_new)
        os.symlink(d_new, self.l_updating)
        run_thread = threading.Thread(target=self.run,
                                      args=[d_new, self.FL_FINISHED])
        if not wait:
            run_thread.setDaemon(True)
        run_thread.start()

