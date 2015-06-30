import baseplugin
import os
import logging
import ftplib
import filecmp
import subprocess

def create():
    return Plugin()


class Plugin(baseplugin.Base):
    def __init__(self):
        baseplugin.Base.__init__(self)

        # update method: scratch or incremental (ie, starting from last version)
        self.method = 'scratch'

        # frequency for checking for updates updates
        # sec, min, hours, day of week (in cron format)
        self.freq(sec='0', min='17', hour='*/1', day='*', dow='*')
        #self.freq(sec='0', min='0', hour='3', day='*', dow='*')
        self.freq_stable(sec='0', min='0', hour='4', day='*', dow='SAT')

        # contact name, email
        self.contact = 'Ross Mccants'
        self.email =  'ross.mccants@unibas.ch'

        self.logger = logging.getLogger(__name__)

    def check_update(self, TMPPATH, LATEST):
        '''
        Make the necessary checks to decide if an update is needed.
        TMPPATH points to a temporary working directory.
        LATEST is directory with the lastet version of the database.
        Return True if that is the case, False otherwise.
        '''
    
        try:
            server = 'ftp.expasy.org'
            path = 'databases/uniprot/current_release/knowledgebase/complete/'
            rfilename = 'reldate.txt'
            lfilename = os.path.join(TMPPATH, rfilename)
            ftp = ftplib.FTP(server)
            ftp.login()
            ftp.cwd(path)
            ftp.retrbinary("RETR " + rfilename, open(lfilename, 'wb').write)
            ftp.quit()
        except Exception, e:
            self.logger.warn("Error downloading {}: {}".format(rfilename, e))
            return False
    
        currentrel = os.path.join(TMPPATH, 'reldate.txt')
        previousrel = os.path.join(LATEST, 'reldate.txt')
    
        if not os.path.isfile(previousrel):
            self.logger.debug('Previous release file not present')
            return True
    
        if filecmp.cmp(currentrel, previousrel):
            self.logger.debug('Release files match. No need to update.')
            return False
        else:
            self.logger.debug('Release files do not match. Updating.')
            return True


    def run(self, WORKPATH):
        '''
        Script to download and expand database in the WORKPATH directory.
        '''
    
        self.logger.info('Step 1/4 (downloading)')
        try:
            server = 'ftp.expasy.org'
            path = 'databases/uniprot/current_release/knowledgebase/complete/'
            rfilename = 'reldate.txt'
            lfilename = os.path.join(WORKPATH, rfilename)
            ftp = ftplib.FTP(server)
            ftp.login()
            ftp.cwd(path)
            ftp.retrbinary("RETR " + rfilename, open(lfilename, 'wb').write)
            ftp.quit()
        except Exception, e:
            self.logger.warn("Error downloading {}: {}".format(rfilename, e))
    
        self.logger.info('Step 2/4 (downloading)')
        try:
            server = 'ftp.expasy.org'
            path = 'databases/uniprot/current_release/knowledgebase/complete/'
            rfilename = 'uniprot_sprot.dat.gz'
            lfilename = os.path.join(WORKPATH, rfilename)
            ftp = ftplib.FTP(server)
            ftp.login()
            ftp.cwd(path)
            ftp.retrbinary("RETR " + rfilename, open(lfilename, 'wb').write)
            ftp.quit()
        except Exception, e:
            self.logger.warn("Error downloading {}: {}".format(rfilename, e))
    
        self.logger.info('Downloading: 3/4')
        try:
            server = 'ftp.expasy.org'
            path = 'databases/uniprot/current_release/knowledgebase/complete/'
            rfilename = 'uniprot_trembl.dat.gz'
            lfilename = os.path.join(WORKPATH, rfilename)
            ftp = ftplib.FTP(server)
            ftp.login()
            ftp.cwd(path)
            ftp.retrbinary("RETR " + rfilename, open(lfilename, 'wb').write)
            ftp.quit()
        except Exception, e:
            self.logger.warn("Error downloading {}: {}".format(rfilename, e))
    
        self.logger.info('Step 3/4 (unzipping)')
        ifilename = os.path.join(WORKPATH, 'uniprot_sprot.dat.gz')
        ofilename = os.path.join(WORKPATH, 'uniprot_sprot.dat')
        fofilename = open(ofilename, 'w')
        subprocess.call(['gzip', '-d', '-c', ifilename], stdout=fofilename)
        fofilename.close()
        os.remove(ifilename)
        ifilename = os.path.join(WORKPATH, 'uniprot_trembl.dat.gz')
        ofilename = os.path.join(WORKPATH, 'uniprot_trembl.dat')
        fofilename = open(ofilename, 'w')
        subprocess.call(['gzip', '-d', '-c', ifilename], stdout=fofilename)
        fofilename.close()
        os.remove(ifilename)
        uniprotdir = os.path.join(WORKPATH, 'UniProt')
        #try:
        #    os.makedirs(uniprotdir)
        #except OSError:
        #    raise
        env = dict(os.environ)
        env.update({'HOME':WORKPATH})
        self.logger.info('Step 4/4 (deflating)')
        with open(os.path.join(WORKPATH, '.embossrc'), 'w') as fconfig:
            config ='''set dpath       {}
set ipath       {}

SET PAGESIZE 4096
SET CACHESIZE 400
RES swiss [
   type: Index
   idlen:  20
   acclen: 15
]
DB uniprot [
         type:          P
         dbalias:       UniProt
         method:        emboss
         release:       "0.0"
         format:        swiss
         dir:           $dpath
         indexdir:      $ipath
         comment:       "UniProt (Swiss-Prot & TrEMBL), all sequences"
]

DB up [
         type:          P
         dbalias:       UniProt
         method:        emboss
         release:       "0.0"
         format:        swiss
         dir:           $dpath
         indexdir:      $ipath
         comment:       "UniProt (Swiss-Prot & TrEMBL), all sequences"
]
'''.format(WORKPATH, WORKPATH)
            fconfig.write(config) 
        command = "dbxflat -auto -dbresource=swiss -idformat=SWISS -fields=id,acc\
            -dbname=UniProt -directory={} -indexoutdir={}\
            -filenames=*.dat".format(WORKPATH, WORKPATH)
        p = subprocess.call(command.split(), env=env)
        os.remove(os.path.join(WORKPATH, '.embossrc'))
    
        self.logger.info('Finished upgrading')
        # TODO: move this out of the user space.
        # write flag indicating download finished
        self.status = self.SGN_FINISHED
        self.logger.debug('LEAVING RUN')
        return
