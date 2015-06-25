import logging
import os


class Base(object):

    def __init__(self):
        self.method = 'scratch'
        self.contact = '' 
        self.email = ''
        self.dependencies = []
        self.logger = logging.getLogger(__name__)

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

    def check_update(self):
        raise Exception('NotImplemented. The method needs to be implemented in subclasses')

    def run(self):
        raise Exception('NotImplemented. The method needs to be implemented in subclasses')


