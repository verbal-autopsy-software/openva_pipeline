Installation Guide
==================

The following instructions guide the installation of the openVA Pipeline on Ubuntu 20.04 operating system.

  .. note::
     To make the installation process easier, all required software can be installed by downloading and running a bash
     script located in the main folder of the openVA_Pipeline repository:

     - `ubuntu_installation.sh <https://github.com/verbal-autopsy-software/openva_pipeline/blob/master/ubuntu_installation.sh>`_

     This script is valid for Ubuntu 20.04 and 22.04.

#. Install Python tools, OpenJDK, **R**, SQLite3, and SQLCipher by typing the following commands at a terminal prompt (indicated by $)

   .. code:: bash

      $ sudo apt update
      $ sudo apt install python3-pip python3-venv openjdk-11-jdk r-base sqlite3 libsqlite3-dev sqlcipher libsqlcipher-dev curl libcurl4-openssl-dev -y


#. Configure OpenJDK with R by running the following command

   .. code:: bash

      $ sudo R CMD javareconf 

#. In a terminal, start **R** by simply typing ``R`` at the prompt, or use ``sudo R`` for system-wide installation of
   the packages.  The necessary packages can be installed (with an internet connection) using the following command:

   .. code:: r

     > install.packages("openVA")

   Note that ``>`` in the previous command indicates the **R** prompt (not part of the actual command).  This command
   may ask the user to select a CRAN mirror (choose a mirror close to your geographic location). After the installation
   of the packages has been completed, you can exit **R** with the following command:

   .. code:: r

     > q('no')

#. Python 3.8 is included in in Ubuntu 20.04 (and Python 3.10 comes with Ubuntu 22.04), but we still need to update the
   package installer, setup a virtual environment, and install the
   `openva-pipeline <https://pypi.org/project/openva-pipeline/>`_ package.  These tasks can be accomplished at the
   terminal as follows:

   .. code:: bash

     $ python3 -m venv venv
     $ source venv/bin/activate
     $ pip install --upgrade pip
     $ pip install --upgrade setuptools
     $ pip install openva-pipeline
     $ deactivate

   We can enter the virtual environment and import the openva-pipeline package with the following terminal commands:

   .. code:: bash

     $ source venv/bin/activate
     $ python

     >>> import openva_pipeline as ovapl   # import package
     >>> help(ovapl)                       # access primary help file (hit q to exit)
     >>> help(ovapl.run_pipeline)          # access help file for a particular function
     >>> quit()                            # return to virtualenv terminal shell

     $ deactivate


#. Install DB Browser for SQLite with the commands (the following commands are taken from the
   `sqlitebrowser GitHub repository <https://github.com/sqlitebrowser/sqlitebrowser/blob/master/BUILDING.md#ubuntu--debian-linux>`_):

   .. code:: bash

      $ sudo apt install build-essential git-core cmake libsqlite3-dev qt5-default qttools5-dev-tools \
          libsqlcipher-dev qtbase5-dev libqt5scintilla2-dev libqcustomplot-dev qttools5-dev
      $ git clone https://github.com/sqlitebrowser/sqlitebrowser
      $ cd sqlitebrowser
      $ mkdir build
      $ cd build
      $ cmake -Dsqlcipher=1 -Wno-dev ..
      $ make
      $ sudo make install
      

Alternative Installation Options
--------------------------------

Using Java JDK (instead of OpenJDK)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Instructions for installing JDK 11 on Linux can be found `here <https://www.javahelps.com/2017/09/install-oracle-jdk-9-on-linux.html>`_.
After installing JDK 11, run the following command at the terminal to properly configure **R**

.. code:: r

   $ sudo R CMD javareconf

and then install the **R** packages (as described above).
