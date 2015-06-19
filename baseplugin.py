import logging
import os


class Base(object):


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


    def contact(self, person, email):
        self.person = person
        self.email = email


    def check_update_stable(self, PATH, FLAG_UPDATE_STABLE):
        logger = logging.getLogger(__name__)
        try:
            os.makedirs(PATH)
        except:
            pass
        flagpath = os.path.join(PATH, FLAG_UPDATE_STABLE)
        open(flagpath, 'w').close()
        logger.debug('Created STABLE {}'.format(flagpath))
        return
    
    
    def check_update_daily(self, PATH, LATEST, FLAG_UPDATE, FLAG_WONT_UPDATE):
        logger = logging.getLogger(__name__)
        os.makedirs(PATH)
        UPDATE = self.check_update(PATH, LATEST)
        if UPDATE:
            flagpath = os.path.join(PATH, FLAG_UPDATE)
        else:
            flagpath = os.path.join(PATH, FLAG_WONT_UPDATE)
            logger.info('No new updates')
        open(flagpath, 'w').close()
        logger.debug('Created DAILY {}'.format(flagpath))
        return

    def check_update(self):
        raise ('NotImplemented. The method needs to be implemented in subclasses')

    def run(self):
        raise ('NotImplemented. The method needs to be implemented in subclasses')


