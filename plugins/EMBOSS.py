import baseplugin
import os
import logging
import ftplib
import filecmp
import subprocess

def create():
    return EMBOSS()


class EMBOSS(baseplugin.Base):
    def __init__(self):
        # frequency for checking if updates are available
        # sec, min, hours, day of week (in cron format)
        self.check_freq('0', '*/2', '*', '*')
        self.check_freq_stable('0', '0', '*/1', '*')
        # contact: name, email
        self.contact('Ross Mccants', 'ross.mccants@unibas.ch')


    def check_update(self, PATH, LATEST):
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


    def run(self, PATH, FLAG_FINISHED):
        '''
        Script to download and expand database in the PATH directory.
        It must write FLAG_FINISHED to indicate that download finished
        '''
    
        logger = logging.getLogger(__name__)
    
        logger.info('Downloading: 1/4')
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
    
        #logger.info('Downloading: 2/4')
        #try:
        #    server = 'ftp.expasy.org'
        #    path = 'databases/uniprot/current_release/knowledgebase/complete/'
        #    rfilename = 'uniprot_sprot.dat.gz'
        #    lfilename = os.path.join(PATH, rfilename)
        #    ftp = ftplib.FTP(server)
        #    ftp.login()
        #    ftp.cwd(path)
        #    ftp.retrbinary("RETR " + rfilename, open(lfilename, 'wb').write)
        #    ftp.quit()
        #except Exception, e:
        #    logging.warn("Error downloading {}: {}".format(rfilename, e))
    
        # logger.info('Downloading: 3/4')
        # try:
        #     server = 'ftp.expasy.org'
        #     path = 'databases/uniprot/current_release/knowledgebase/complete/'
        #     rfilename = 'uniprot_trembl.dat.gz'
        #     lfilename = os.path.join(PATH, rfilename)
        #     ftp = ftplib.FTP(server)
        #     ftp.login()
        #     ftp.cwd(path)
        #     ftp.retrbinary("RETR " + rfilename, open(lfilename, 'wb').write)
        #     ftp.quit()
        # except Exception, e:
        #     logging.warn("Error downloading {}: {}".format(rfilename, e))
    
#        logger.info('Expanding: 4/4')
#        ifilename = os.path.join(PATH, 'uniprot_sprot.dat.gz')
#        ofilename = os.path.join(PATH, 'uniprot_sprot.dat')
#        fofilename = open(ofilename, 'w')
#        subprocess.call(['gzip', '-d', '-c', ifilename], stdout=fofilename)
#        fofilename.close()
#        os.remove(ifilename)
#        #filename = 'uniprot_trembl.dat.gz'
#        #subprocess.call(['gzip', '-d', filename])
#        uniprotdir = os.path.join(PATH, 'UniProt')
#        try:
#            os.makedirs(uniprotdir)
#        except OSError:
#            raise
#        env = dict(os.environ)
#        env.update({'HOME':PATH})
#        with open(os.path.join(PATH, '.embossrc'), 'w') as fconfig:
#            config ='''set dpath       {}
#set ipath       {}
#
#SET PAGESIZE 4096
#SET CACHESIZE 400
#RES swiss [
#   type: Index
#   idlen:  20
#   acclen: 15
#]
#DB uniprot [
#         type:          P
#         dbalias:       UniProt
#         method:        emboss
#         release:       "0.0"
#         format:        swiss
#         dir:           $dpath/UniProt
#         indexdir:      $ipath/UniProt
#         comment:       "UniProt (Swiss-Prot & TrEMBL), all sequences"
#]
#
#DB up [
#         type:          P
#         dbalias:       UniProt
#         method:        emboss
#         release:       "0.0"
#         format:        swiss
#         dir:           $dpath
#         indexdir:      $ipath
#         comment:       "UniProt (Swiss-Prot & TrEMBL), all sequences"
#]
#'''.format(PATH, PATH)
#            fconfig.write(config) 
#        command = "dbxflat -auto -dbresource=swiss -idformat=SWISS -fields=id,acc\
#            -dbname=UniProt -directory={} -indexoutdir={}\
#            -filenames=*.dat".format(PATH, uniprotdir)
#        p = subprocess.call(command.split(), env=env)
#        os.remove(os.path.join(PATH, '.embossrc'))
    
        # write flag indicating download finished
        open(os.path.join(PATH, FLAG_FINISHED), 'w').close()
        return
