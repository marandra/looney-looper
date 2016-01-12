ScheduleDB
==========
Marcelo Raschi (marcelo.raschi@unibas.ch)
https://github.com/malandra/scheduledb


Description
-----------
ScheduleDB is a system that schedule updates of databases based on the frequency and the scripts provided by the user. The parameters and the scripts are provided as python programs, which are loaded at the initial run of the system. The system takes care of the directory structure for the different versions of a database, and generates symbolic links to the current version as well as to other versions if requested by the user.

What's New
----------

Usage
-----
Once install, the scheduledb scrip should be installed and in the path.

Install
-------

Creating a directory structure consistent with the default configuraton file
$ mkdir ~/databases
$ mkdir ~/databases/.system
$ mkdir ~/databases/.store
$ mkdir ~/databases/.plugins
$ cd ~/databases/.system
$ virtualenv venv
$ pip install scheduledb
$ scheduledb

The setup script run by pip will install the script in ~/databases/venv/bin/
The program by default uses the configuration file in ~/databases/venv/lib/python2.7/site-packages/scheduledb/scheduledb.ini
It also received the location of and alternative config file as argument
usage: scheduledb [-h] [-c conffile]
python_installation/site-package/scheduledb/scheduledb.ini

From source
$ cd ~/source
$ git clone http://gitlab/scheduledb
$ python setup sdist

Continue with the instructionis for installing using pip, replacing
$ pip install scheduledb
with
$ pip install ~/source/scheduledb/dist/scheduledb-0.0.1.tar.gz
look for the correct version

Set up
------
The set up is done via a configurarion file read at running time. There is a default configurationfile loaded at run time. It is possible to pass and alternate file as argument.

Getting plugins
---------------
Plugins are available from a git repository. After downloading, it might be necessary some configuration to adjust to particular needs, but should be a good start.
They also work as a good starting point for developing new plugins.

At the moment, the available plugins are 
1) Uniprot
https://git.scicore.unibas.ch/raschi/sdbplugin-uniprot
2) PDB
https://git.scicore.unibas.ch/raschi/sdbplugin-pdb
3) igenomes
https://git.scicore.unibas.ch/raschi/sdbplugin-igenomes
