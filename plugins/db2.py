import baseplugin
import os
import logging
import filecmp
import shutil
import time


def create():
    return Plugin()


class Plugin(baseplugin.Base):
    def __init__(self):
        baseplugin.Base.__init__(self)

        # scratch, incremental, long-term
        self.method = 'incremental'

        # append dependencies, ie databases that must be updated beforehand
        self.dep['db3'] = '0'

        # frequency for checking if updates are available
        # sec, min, hours, day of week (in cron format)
        self.freq(sec='*/20', min='*', hour='*', dow='*')
        self.freq_stable(sec='0', min='*/2', hour='*', dow='*')

        # contact name, email
        self.contact = 'Juan Perez'
        self.email = 'juan.perez@unibas.ch'

        self.logger = logging.getLogger(__name__)

    def check_update(self, PATH, LATEST):
        '''
        Make the necessary checks to decide if an update is needed.
        PATH points to a temporary working directory.
        LATEST is directory with the lastet version of the database.
        Return True if that is the case, False otherwise.
        '''

        rfilename = 'database'
        rpath = '/import/bc2/data/test/remotedb'
        currentrel = os.path.join(rpath, rfilename)
        previousrel = os.path.join(LATEST, rfilename)

        if not os.path.isfile(previousrel):
            self.logger.debug('Previous release file not present. Updating')
            return True

        if filecmp.cmp(currentrel, previousrel):
            self.logger.debug('Release files match. No need to update.')
            return False
        else:
            self.logger.debug('Release files do not match. Updating.')
            return True

    def run(self, WORK_PATH):
        '''
        Script to download and expand database in the PATH directory.
        It must write FLAG_FINISHED to indicate that download finished
        '''

        self.logger.info('Downloading')

        rfilename = 'database'
        rpath = '/import/bc2/data/test/remotedb'
        currentrel = os.path.join(rpath, rfilename)
        previousrel = os.path.join(WORK_PATH, rfilename)
        shutil.copy(currentrel, previousrel)
        previousrel = os.path.join(WORK_PATH, 'localdatabase')
        with open(previousrel, 'a') as f:
            f.write(open(currentrel, 'r').read())
        time.sleep(5)

        # TODO: move this out of the user space.
        # write flag indicating download finished
        #open(os.path.join(PATH, FLAG_FINISHED), 'w').close()
        self.status = self.SGN_FINISHED
        return
