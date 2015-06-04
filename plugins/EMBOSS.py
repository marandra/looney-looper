import os
import logging
import ftplib
import filecmp

second = '0'
minute = '*/10'
hour = '*'
day_of_week = '*'
stable_second = '0'
stable_minute = '0'
stable_hour = '*/1'
stable_day_of_week = '*'
person = 'Ross Mccants'
email = 'ross.mccants@unibas.ch'


def check_update(PATH, LATEST):
    '''
    Make the necessary checks to decide if an update is needed.
    PATH points to a temporary working directory.
    LATEST is directory with the lastet version of the database.
    Return True if that is the case, False otherwise.
    '''

    logger = logging.getLogger(__name__)

    try:
        server = 'ftp.expasy.org'
        path = 'databases/uniprot/current_release/knowledgebase/complete/'
        rfilename = 'reldate.txt'
        lfilename = os.path.join(PATH, rfilename)
        ftp = ftplib.FTP(server)
        ftp.login()
        ftp.cwd(path)
        ftp.retrbinary("RETR " + rfilename, open(lfilename, 'wb').write)
        ftp.quit()
    except Exception, e:
        logging.warn("Error downloading {}: {}".format(rfilename, e))
        return False

    currentrel = os.path.join(PATH, 'reldate.txt')
    previousrel = os.path.join(LATEST, 'reldate.txt')

    if not os.path.isfile(previousrel):
        logger.debug('Previous release file not present')
        return True

    if filecmp.cmp(currentrel, previousrel):
        logger.debug('Release files match. No need to update.')
        return False
    else:
        logger.debug('Release files do not match. Updating.')
        return True


def run(PATH, FLAG_FINISHED):
    '''
    Script to download and expand database in the PATH directory.
    It must write FLAG_FINISHED to indicate that download finished
    '''

    logger = logging.getLogger(__name__)

    logger.debug('Downloading: 1/4')
    try:
        server = 'ftp.expasy.org'
        path = 'databases/uniprot/current_release/knowledgebase/complete/'
        rfilename = 'reldate.txt'
        lfilename = os.path.join(PATH, rfilename)
        ftp = ftplib.FTP(server)
        ftp.login()
        ftp.cwd(path)
        ftp.retrbinary("RETR " + rfilename, open(lfilename, 'wb').write)
        ftp.quit()
    except Exception, e:
        logging.warn("Error downloading {}: {}".format(rfilename, e))

    logger.debug('Downloading: 2/4')
    try:
        server = 'ftp.expasy.org'
        path = 'databases/uniprot/current_release/knowledgebase/complete/'
        rfilename = 'uniprot_trembl.dat.gz'
        lfilename = os.path.join(PATH, rfilename)
        ftp = ftplib.FTP(server)
        ftp.login()
        ftp.cwd(path)
        ftp.retrbinary("RETR " + rfilename, open(lfilename, 'wb').write)
        ftp.quit()
    except Exception, e:
        logging.warn("Error downloading {}: {}".format(rfilename, e))

    logger.debug('Downloading: 3/4')
    try:
        server = 'ftp.expasy.org'
        path = 'databases/uniprot/current_release/knowledgebase/complete/'
        rfilename = 'uniprot_sprot.dat.gz'
        lfilename = os.path.join(PATH, rfilename)
        ftp = ftplib.FTP(server)
        ftp.login()
        ftp.cwd(path)
        ftp.retrbinary("RETR " + rfilename, open(lfilename, 'wb').write)
        ftp.quit()
    except Exception, e:
        logging.warn("Error downloading {}: {}".format(rfilename, e))

    logger.debug('Expanding: 4/4')
    # dbxflat -auto -dbresource=swiss -idformat=SWISS -fields=id,acc -dbname=UniProt -directory=/import/bc2/home/smng-prodA/nobackup/pending/ -filenames='*.dat

    # write flag indicating download finished
    open(os.path.join(PATH, FLAG_FINISHED), 'w').close()
    return

############################################


def check_update_stable(PATH, LATEST, FLAG_UPDATE_STABLE):
    logger = logging.getLogger(__name__)
    os.makedirs(PATH)
    flagpath = os.path.join(PATH, FLAG_UPDATE_STABLE)
    open(flagpath, 'w').close()
    logger.debug('Created STABLE {}'.format(flagpath))
    return


def check_update_daily(PATH, LATEST, FLAG_UPDATE, FLAG_WONT_UPDATE):
    logger = logging.getLogger(__name__)
    os.makedirs(PATH)
    UPDATE = check_update(PATH, LATEST)
    if UPDATE:
        flagpath = os.path.join(PATH, FLAG_UPDATE)
    else:
        flagpath = os.path.join(PATH, FLAG_WONT_UPDATE)
        logger.info('No new updates')
    open(flagpath, 'w').close()
    logger.debug('Created DAILY {}'.format(flagpath))
    return
