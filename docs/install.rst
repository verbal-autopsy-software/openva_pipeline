Installation Guide
==================

The following instructions guide the installation of the openVA Pipeline on Ubuntu 20.04 operating system.

  .. note::
     To make the installation process easier, all of the required software can be installed by downloading and running the bash script
     `install_software_ubuntu_20_04.sh <https://https://github.com/verbal-autopsy-software/openva_pipeline/blob/master/install_software_ubuntu_20_04.sh>`_
     located in the main folder of the openVA_Pipeline repository.

#. Add the apt key (signed by Michael Rutter) to authenticate R package from CRAN and add
   the CRAN repository to get the latest version of R (for more details see CRAN page:
   `Ubuntu packages for R <https://cran.r-project.org/bin/linux/ubuntu/README.html>`_).

   .. code:: bash

     $ sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9
     $ sudo bash -c "echo 'deb https://cloud.r-project.org/bin/linux/ubuntu focal-cran40/' >> /etc/apt/sources.list"

#. Install Python, OpenJDK, **R**, SQLite3, SQLCipher, and Git by typing the following commands at a terminal prompt (indicated by $)

   .. code:: bash

      $ sudo apt update
      $ sudo apt install python3-pip openjdk-11-jdk r-base sqlite3 libsqlite3-dev sqlcipher libsqlcipher-dev curl libcurl4-openssl-dev -y

#. (*optional*) Download the `ODK-Briefcase-v1.18.0.jar <https://github.com/opendatakit/briefcase/releases>`_ file to a folder that will
   serve as the openVA_Pipeline working directory.

#. Configure OpenJDK with R by running the following command

   .. code:: bash

      $ sudo R CMD javareconf 

#. In a terminal, start **R** by simply typing ``R`` at the prompt, or use ``sudo R`` for system-wide installation of
   the packages.  The necessary packages can be installed (with an internet connection) using the following command:

   .. code:: r

     > install.packages("openVA")

   Note that ``>`` in the previous command indicates the **R** prompt (not part of the actual command).  This command will
   prompt the user to select a CRAN mirror (choose a mirror close to your geographic location), as well as a location for the
   library where the R packages will be installed (this does not happen if R is run as sudo).  After the installation
   of the packages has been completed, you can exit **R** with the following command:

   .. code:: r

     > q('no')

#. Python version 3.8 is included in the Ubuntu 20.04, but we still need to update the
   package installer, setup a virtual environment with `Pipenv <https://pipenv.readthedocs.io/en/latest>`_, and install
   the `openva-pipeline <https://pypi.org/project/openva-pipeline/>`_ package.  These tasks can be accomplished at the terminal
   as follows:

   .. code:: bash

     $ pip3 install --upgrade pip --user
     $ hash -d pip3
     $ pip3 install --upgrade setuptools --user
     $ pip3 install pipenv --user
     $ pipenv install openva-pipeline

   We can enter the virtual environment and import the openva-pipeline package with the following terminal commands:

   .. code:: bash

     $ pipenv shell
     $ python


     >>> import openva_pipeline as ovaPL   # import package
     >>> help(ovaPL)                       # access primary help file (hit q to exit)
     >>> help(ovaPL.runPipeline)           # access help file for a particular function
     >>> quit()                            # return to virtualenv terminal shell

     $ exit


#. Install DB Browser for SQLite with the commands

   .. code:: bash

      $ sudo apt install sqlitebrowser -y


Alternative Installation Options
--------------------------------

Using Java JDK (instead of OpenJDK)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Instructions for installing JDK 11 on Linux can be found `here <https://www.javahelps.com/2017/09/install-oracle-jdk-9-on-linux.html>`_.
After installing JDK 11, run the following command at the terminal to properly configure **R**

.. code:: r

   $ sudo R CMD javareconf

and then install the **R** packages (as described above).


Installing the Pipeline package with using a virtual environment (and pipenv)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Simply use ``pip3`` to install the openva-pipeline package as follows

.. code:: bash

   $ pip3 install openva-pipeline --user
