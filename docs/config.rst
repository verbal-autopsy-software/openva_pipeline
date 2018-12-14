Pipeline Configuration
======================

#. **Create the SQLite database**: The openVA pipeline uses an SQLite database to access configuration settings for ODK Aggregate, openVA in R,
   and DHIS2. Error and log messages are also stored to this database, along with the VA records downloaded from ODK Aggregate and
   the assigned COD.

   #. The necessary tables and schema are created in the SQL file pipelineDB.sql, which can be downloaded from the
      `OpenVA_Pipeline GitHub webpage <https://github.com/D4H-CRVS/OpenVA_Pipeline/pipelineDB.sql>`_.  Create the SQLite database in the
      same folder as the file *pipeline.py*.

   #. Use SQLCipher to create the pipeline database, assign an encryption key, and populate the database using the following commands
      (note that the ``$`` is the terminal prompt and ``sqlite>`` is the SQLite prompt, i.e., not part of the commands).

    .. code:: bash

       $ sqlcipher
       sqlite> .open Pipeline.db
       sqlite> PRAGMA key=encryption_key;
       sqlite> .read "pipelineDB.sql"
       sqlite> .tables
       sqlite> -- take a look --
       sqlite> .schema ODK_Conf
       sqlite> SELECT odkURL from ODK_Conf;
       sqlite> .quit

    Note how the pipeline database is encrypted, and can be accessed via with SQLite command: ``PRAGMA key = "encryption_key;"``

    .. code:: bash

       $ sqlcipher
       sqlite> .open Pipeline.db
       sqlite> .tables

       Error: file is encrypted or is not a database

       sqlite> PRAGMA key = "encryption_key";
       sqlite> .tables
       sqlite> .quit

   3. Open the file *pipeline.py* and edit the first two lines of code by including the name of the pipeline SQLite database (the default
      is *Pipeline.db*) and the encryption password.  The lines in *pipeline.py* are:

    .. code:: python

       #----------------------------------------------#
       # User Settings
       sqlitePW = "enilepiP"
       dbName   = "Pipeline.db"
       #----------------------------------------------#

#. **Configure Pipeline**: The pipeline connects to ODK Aggregate and DHIS2 servers and thus requires usernames, passwords, and URLs.
   Arguments for openVA should also be supplied. We will use
   `DB Browser for SQLite <https://github.com/sqlitebrowser/sqlitebrowser/blob/master/BUILDING.md>`_ to configure these settings. Start
   by launching DB Browser from the terminal, which should open the window below ``$ sqlitebrowser``


      .. image:: Screenshots/dbBrowser.png

   Next, open the database by selecting the menu options: *File* -> *Open Database...*


      .. image:: Screenshots/dbBrowser_open.png

   and navigate to the *Pipeline.db* SQLite database and click the *Open* button.  This will prompt you to enter in encryption password.


      .. image:: Screenshots/dbBrowser_encryption.png
    
   #. **ODK Configuration**: To configure the pipeline connection to ODK Aggregate, click on the *Browse Data* tab and select the
      ODK\_Conf table as shown below.

         .. image:: Screenshots/dbBrowser_browseData.png


         .. image:: Screenshots/dbBrowser_odk.png

      Now, click on the *odkURL* column, enter the URL for your ODK Aggregate server, and click *Apply*.


         .. image:: Screenshots/dbBrowser_odkURLApply.png

      Similarly, edit the *odkUser*, *odkPass*, and *odkFormID* columns so they contain a valid user name, password, and Form ID
      (see Form Management on ODK Aggregate server) of the VA questionnaire of your ODK Aggregate server.

   #. **openVA Configuration**: The pipeline configuration for openVA is stored in the *Pipeline\_Conf* table. Follow the steps described
      above (in the ODK Aggregate Configuration section) and edit the following columns:

      * *workingDirectory* -- the directory where the pipeline files (i.e., *pipeline.py*, *Pipeline.db* and the ODK Briefcase
        application, *ODK-Briefcase-v1.10.1.jar*).  Note that the pipeline will create new folders and files in this working directory,
        and must be run by a user with privileges for writing files to this location.   

      * *openVA\_Algorithm* -- currently, there are only three acceptable values for the alogrithm: ``Insilico``, ``InterVA`` or ``SmartVA``

      * *algorithmMetadataCode* -- this column captures the necessary inputs for producing a COD, namely the VA questionnaire, the
        algorithm, and the symptom-cause information (SCI) (see [below](#SCI) for more information on the SCI).  Note that there are also
        different versions (e.g., InterVA 4.01 and InterVA 4.02, or WHO 2012 questionnare and the WHO 2016 instrument/questionnaire).  It is
        important to keep track of these inputs in order to make the COD determination reproducible and to fully understand the assignment
        of the COD.  A list of all algorith metadata codes is provided in the *dhisCode* column in the *Algorithm\_Metadata\_Options* table.
        The logic for each code is

        algorith|algorithm version|SCI|SCI version|instrument|instrument version

      * *codSource* -- both the InterVA and InSilicoVA algorithms return CODs from a list produced by the WHO, and thus this column should
        be left at the default value of ``WHO``.

   #. **DHIS2 Configuration**: The pipeline configuration for DHIS2 is located in the *DHIS\_Conf* table, and the following columns should
      be edited with appropriate values for your DHIS2 server.

      * *dhisURL* --  the URL for your DHIS2 server 
      * *dhisUser* -- the username for the DHIS2 account
      * *dhisPass* -- the password for the DHIS2 account
      * *dhisOrgUnit* -- the Organization Unit (e.g., districts) UID to which the verbal autopsies are associated. The organisation unit
        must be linked to the Verbal Autopsy program.  For more details, see the DHIS2 Verbal Autopsy program
        `installation guide <https://github.com/SwissTPH/dhis2_va_draft/blob/master/docs/Installation.md>`_

#. **SmartVA Configuration**: The pipeline can also be configured to run SmartVA using the Python application available from the GitHub repository `ihmeuw/SmartVA-Analyze <https://github.com/ihmeuw/SmartVA-Analyze>`_.

   #. Install the SmartVA-Analyze application from the repository: `https://github.com/ihmeuw/SmartVA-Analyze <https://github.com/ihmeuw/SmartVA-Analyze>`_ and save it in the pipeline's working directory (see below).
   
   #. Update the *Pipeline\_Conf* table in the SQLite database with the following values:

      * *workingDirectory* -- the directory where the pipeline files are stored.

      * *openVA\_Algorithm* -- set this field to ``SmartVA``

      * *algorithmMetadataCode* -- set this field to the appropriate SCI, e.g.
      
        SmartVA|2.0.0_a8|PHMRCShort|1|PHMRCShort|1

      * *codSource* -- set this field to``Tariff``.
