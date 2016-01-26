ScheduleDB
==========
Marcelo Raschi (marcelo.raschi@unibas.ch)

Github repository_.

.. _repository: https://github.com/

Description
-----------
ScheduleDB is a system that schedule updates of databases based on the frequency and the scripts provided by the user. The parameters and the scripts are provided as python programs, which are loaded at the initial run of the system.

The system takes care of the directory structure for the different versions of a database, and generates symbolic links to the current version as well as to other versions if requested by the user.

What's New
----------
v0.2

- Modified to use *pip* packaging system

Usage
-----
Once installed, the *scheduledb* script should be installed and searchable in the path.

Installation
------------
Let's assume the following locations (more on this later):

- datatabase repository: *~/databases*
- time-tagged databases: *~/databases/.store*
- system: *~/databases/.system*
- plugin repository: *~/databases/.plugins*

The first step is to create the desired directory structure:

.. code-block::

  $ mkdir ~/databases
  $ mkdir ~/databases/.system
  $ mkdir ~/databases/.store
  $ mkdir ~/databases/.plugins
  $ cd ~/databases/.system
  $ virtualenv venv
  $ pip install scheduledb
  $ scheduledb

The setup script run by pip will install the script in *~/databases/venv/bin/* and all the necessary dependencies

The second step is to have a configuration file that is consistent with the directory structure. The program takes as optionnal argument a configuration file

-h              print this message
-c configfile   location of the configuration file to use

If no configuration file is provided, scheduledb will use the default configuration file located at  *~/databases/venv/lib/python* <python-version> */site-packages/scheduledb/scheduledb.ini*
