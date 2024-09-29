.. _backup:

******
Backup
******

As explained in the :ref:`projectconf` section, jobflow-remote uses a MongoDB
database to store the information about the state of Jobs and Flows. This is
defined in the ``queue`` section of the project configuration.
In several circumstances it may be required to perform a backup of this
database. For this reason jobflow-remote offers an option to create a dump
of the relevant collections for a project and restore it if needed.

.. warning::
    This functionality does **not create a backup of the** ``JobStore`` **containing
    the output of the workflows**. Since the output store can be any kind of ``Store``
    and the result may be split in the ``additional_stores``, if a backup is needed
    it will be required to do that through the ``JobStore`` or directly with
    the storage system.

There are two options to create and restore a backup. The default relies on the official
MongoDB tools: ``mongodump`` and ``mongorestore``. For this to work the
`MongoDB database tools <https://www.mongodb.com/docs/database-tools>`_  need to be
installed. The connection details provided in the project configuration will be used
to executed the commands. This is the preferred option, since it is faster and also
dumps and restores all the metadata of the collections. However, not all the connection
options defined in the ``queue`` Store may be supported or it may be not possible
to install the tools. For this reason a second option, based on a pure python implementation
is also available. This can be activated by selecting the ``--python`` option from
the CLI.

.. warning::
    The python version of the backup and restore will not preserve the metadata of the
    collection. After restoring a backup with this option it would be better to
    regenerate the standard indexes using the ``jf admin index rebuild`` command.

.. note::
    It is of course possible to manually create a backup using the MongoDB tools.
    This jobflow-remote feature is meant to ease the procedure by automatically
    selecting the appropriate collections to backup.

Create a backup
===============

A backup can be created with the command::

    jf backup create

As already mentioned, this will use the ``mongodump`` executable, unless the ``--python``
option is specified. It is possible to specify the destination path of the backup and the
output folder contains the ``jobs.bson``, ``flows.bson`` and ``jf_auxiliary.bson``
files. If the ``mongodump`` command is used, the folder will also contain the metadata
files for each collection. It is also possible to request that the backup files will
be gzipped, by adding the ``--compress`` option.

.. note::
    The folder creation follows the convention of the ``mongodump`` executable, so
    inside the folder specified in the ``create`` command there will be a subfolder
    with the name of the database.

.. note::
    The name of the files will be the standard ones, even if the names of the collections
    defined in the project configuration file are different.

Restore a backup
================

To restore a backup the following command can be used::

    jf backup restore /path/to/backup/folder

The path should point to the folder containing the bson files generated during the creation.
The code will automatically determine if the files are zipped, based on their extension.

.. note::
    The name of the target collection are determined by the values defined in the project
    settings, not by the names of the files, nor by the names of the collections from
    which the backup was created.

.. note::
    The backup can be restored only in an empty database. The code will raise an error
    if the target database already contains jobs and flows.
