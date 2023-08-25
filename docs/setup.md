# Software Requirements & Installation Guide for Ubuntu Server 20.04 & 22.04

Note: To make the installation process easier, all required software can be installed by downloading and running 
the bash script [install_software_ubuntu.sh](https://github.com/verbal-autopsy-software/openva_pipeline/blob/master/ubuntu_installation.sh)
located in the main folder of this repository.  This script works for Ubuntu versions 20.04 and 22.04.

1. The following software is required by the openVA Pipeline: [Python 3.8 (or higher)]((https://www.python.org/downloads/)),
   [OpenJDK](http://openjdk.java.net) (or [Java JDK 7 or 8]()), [R](https://cran.r-project.org), [SQLite3](https://www.sqlite.org),
   and [SQLCipher](https://github.com/sqlcipher/sqlcipher).  For installation purposes, it is also useful to install
   [PIP](https://pypi.python.org/pypi/pip) (tool for installing Python packages).
    - Run the following commands at a terminal prompt (indicated by $)

    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.bash}
    $ sudo apt update
    $ sudo apt install python3-pip python3-venv openjdk-11-jdk r-base sqlite3 libsqlite3-dev sqlcipher libsqlcipher-dev curl libcurl4-openssl-dev -y
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    - Configure R to find the local openjdk (or Java) installation:

    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.bash}
    $ sudo R CMD javareconf
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. R Package:  [openVA](https://cran.r-project.org/web/packages/openVA/index.html)
Enter the terminal command ```sudo R```, which will start R.  Install the openVA R package using the following command:

    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.r}
    > install.packages("openVA")
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Note that `>` in the previous command indicates the R prompt and not part of the actual command.  This command may
    ask the user to select a CRAN mirror (choose a mirror close to your geographic location).  After the installation
    of the packages has been completed, you can exit R with the following command:
    
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.r}
    > q('no')
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Python packages & updates
    - Python 3 is pre-installed on Ubuntu 20.04 (Python 3.8) and 22.04 (Python 3.10), but additional packages,
    modules, and updates are needed.  It is also a good idea to isolate the Python tools used by the openVA Pipeline
    into a virtual environment.  These steps can be accomplished with the following command at a terminal:

        ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.bash}
        $ mkdir /path/to/folder_for_pipeline
        $ cd /path/to/folder_for_pipeline
        $ python3 -m venv venv
        $ source venv/bin/activate
        $ pip install --upgrade pip
        $ pip install --upgrade setuptools
        $ pip install openva-pipeline
        $ deactivate
        ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        
        Note: Once you activate the virtual environment, the terminal prompt should change to include `(venv)`.  The
        command `deactivate` will exit the virtual environment.  

1. (optional) [DB Browser for SQLite](https://github.com/sqlitebrowser/sqlitebrowser/blob/master/BUILDING.md): this
tool is useful for configuring the SQLite Database when a desktop environment (e.g., Gnome or KDE) is available.  DB
Browser can be installed using the following commands:

    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.bash}
    $ sudo apt update
    $ sudo apt install sqlitebrowser -y
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Pipeline Setup

1. Create the SQLite **transfer database**: The openVA Pipeline uses an SQLite database, referred to as the transfer db,
to access configuration settings for ODK Central, openVA, and DHIS2. Error and log messages are also stored to this
database, along with the VA records downloaded from ODK Central and the assigned COD.  There are two ways to do this
(you only need to do one).

   * **Python Version**:  In a terminal, change to the folder where you will run the openVA Pipeline (i.e., the folder
   where downloads from ODK Central will be saved, and openVA logs are crated).  Activate your virtual environment
   (with `source venv/bin/activate`), start Python (with `python3`), and run the following commands

      ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.python}
      >>> import openva-pipeline as ovapl
      >>> ovapl.create_transfer_db("name_of_dbase.db", ".", "enter_your_password_here")
      >>> quit()
      ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     
     **You will need to remember the password to run the openVA Pipeline and access the transfer db!**

   * **SQL Version**: Download the SQL for creating the transfer DB from the
   [openVA Pipeline GitHub repository](https://github.com/verbal-autopsy-software/openva_pipeline/blob/master/openva_pipeline/sql/pipelineDB.sql),
   and make your customizations (e.g., adding the URL for ODK Central) to the SQL using a text editor.  Next, use
   SQLCipher to create the Pipeline database, assign an encryption key, and populate the database using the
   following commands (note that the `$` is the terminal prompt and `sqlite>` is the SQLite prompt, i.e., not part of
   the commands).
       
      ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.bash}
      $ sqlcipher
      sqlite> .open Pipeline.db
      sqlite> PRAGMA key = "enter_your_password_here";
      sqlite> .read "pipelineDB.sql"
      sqlite> .tables
      sqlite> -- take a look --
      sqlite> .schema ODK_Conf
      sqlite> SELECT odkURL from ODK_Conf
      sqlite> .quit
      ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

     **You will need to remember the password to run the openVA Pipeline and access the transfer db!**

   * You can use the password to access the transfer db with the SQLite command: <br/>
      `PRAGMA key = "enter_your_password_here";`

      ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.bash}
      $ sqlcipher
      sqlite> .open Pipeline.db
      sqlite> .tables

      Error: file is encrypted or is not a database

      sqlite> PRAGMA key = "enter_your_password_here";
      sqlite> .tables

      Tables will now be listed

      sqlite> .quit
      ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. **Configure Pipeline**: The Pipeline connects to ODK Collect and DHIS2 servers and thus requires usernames,
passwords, and URLs.  Arguments for openVA should also be supplied. We will use
[DB Browser for SQLite](https://github.com/sqlitebrowser/sqlitebrowser/blob/master/BUILDING.md) to configure these
settings. Start by launching DB Browser from the terminal, which should open the window below `$ sqlitebrowser`
    
   ![](Screenshots/dbBrowser.png)
    
   Next, open the database by selecting the menu options: _File_ -> _Open Database..._
    
   ![](Screenshots/dbBrowser_open.png)
    
   and navigate to the _Pipeline.db_ SQLite database and click the _Open_ button.  This will prompt you to enter in
   encryption password.
    
   ![](Screenshots/dbBrowser_encryption.png)
    
   1. **ODK Configuration**: To configure the Pipeline connection to ODK Central, click on the _Browse Data_ tab and
   select the ODK_Conf table as shown below.
       
      ![](Screenshots/dbBrowser_browseData.png)
      
      ![](Screenshots/dbBrowser_odk.png)
       
      Now, click on the <em>odkURL</em> column, enter the URL for your ODK Central server, and click <em>Apply</em>.
       
      ![](Screenshots/dbBrowser_odkURLApply.png)
       
      Similarly, edit the <em>odkUser</em>, <em>odkPassword</em>, <em>odkFormID</em>, and <em>odkProjectNumber</em>
      columns to contain a valid username, password, Form ID of the VA questionnaire
      (see [Form Management on ODK Central server](https://docs.getodk.org/central-forms/)), and Project Number on 
      your ODK Central server.  It is not recommended, but if you are using an ODK Aggregate server, then you need to
      set the <em>odkUseCentral</em> filed to False.
    
   1. **Pipeline Configuration**: These settings are stored in the <em>Pipeline_Conf</em> table.
   Follow the steps described above (in the ODK Central Configuration section) and edit the following columns:
      - __workingDirectory__ -- the directory where the Pipeline files are located (e.g.,  _Pipeline.db_).  Note that
      the Pipeline will create new folders and files in this working directory, and must be run by a user with
      privileges for writing files to this location.   
      - __algorithm__ -- which algorithm to use for assigning causes of death; acceptable values are `InterVA`,
      `InSilicoVA`, or `SmartVA`
      - __algorithmMetadataCode__ -- this column captures the necessary inputs for producing a COD, namely the VA
      questionnaire, the algorithm, and the symptom-cause information (SCI) (see [below](#SCI) for more information on
      the SCI).  Note that there are also different versions (e.g., InterVA 4.01 and InterVA 4.02, or WHO 2012
      questionnare and the WHO 2016 instrument/questionnaire).  It is important to keep track of these inputs in order
      to make the COD determination reproducible and to fully understand the assignment of the COD.  A list of all
      _algorithmMetadataCodes_ is provided in the _dhisCode_ column in the _Algorithm_Metadata_Options_ table.  The
      logic for each code is </br> algorith|algorithm version|SCI|SCI version|instrument|instrument version.
      - __codSource__ -- both the InterVA and InSilicoVA algorithms return CODs from a list produced by the WHO, and
      thus this column should be left at the default value of `WHO`.
   1. **DHIS2 Configuration**: The Pipeline configuration for DHIS2 is located in the _DHIS\_Conf_ table, and the
   following columns should be edited with appropriate values for your DHIS2 server.
      - __dhisURL__ --  the URL for your DHIS2 server 
      - __dhisUser__ -- the username for the DHIS2 account
      - __dhisPassword__ -- the password for the DHIS2 account
      - __dhisOrgUnit__ -- the Organization Unit (e.g., districts) UID to which the verbal autopsies are associated.
      The organization unit must be linked to the Verbal Autopsy program.  For more details, see the DHIS2 Verbal
      Autopsy program [installation guide](https://github.com/verbal-autopsy-software/DHIS2_VA_program/blob/master/docs/2.30-SingleEvent/Installation.md)
      Alternatively, if there are columns in your ODK form that identify the organization unit of each VA record, then
      include the column names in this field (e.g., "Region, District, Tract").  For this option, simply use the final
      part of the ODK column name.  For example, if a column is labeled "consented-deceased_CRVA-in_on_deceased-Region",
      only include the last part (Region) in the dhisOrgUnit field.

# Miscellaneous notes

## <a name="SCI"> Symptom-Cause Information </a>

A key component of automated cause assignment methods for VA is the symptom-cause information (SCI) that describes how VA symptoms are
related to each cause. It is likely that the relationships of VA symptoms to causes vary in important ways across space and
between administrative jurisdictions, and they are likely to change through time as new diseases and conditions emerge and as
treatments become available. Consequently, automated cause assignment algorithms used for mortality surveillance should optimally
rely on representative SCI that is locally and continuously updated.  Furthermore, it is vital to track the SCI used for COD 
assignment to enable reproducibility and to fully understand the assignment of the COD.
