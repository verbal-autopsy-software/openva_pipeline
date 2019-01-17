Setting Up and Running the 
==========================
**openVA Pipeline**
===================

**Quick Demonstration**
-----------------------

Once Python3, Java, R, and openVA have been installed on your computer, there
are only a few steps needed to install and demonstrate the openVA Pipeline.

#. Start by opening a terminal and changing to the directory that will serve
   as the working directory for the openVA Pipline.  The working directory
   for this example will be a folder called Pipeline located in the user's
   home directory

   .. code:: bash

      $ mkdir -p $HOME/Pipeline
      $ cd $HOME/Pipeline
      $ pipenv --python 3.5

   The last (recommended) step uses
   `Pipenv <https://pipenv.readthedocs.io/en/latest/>`_ to create a virtual
   environment for the pipeline, which should look like the following screenshot


   .. image:: Screenshots/pipenvSetup.png


#. Use Pipenv to install the `openVA Pipeline <https://pypi.org/project/openva-pipeline/>`_
   package from PyPI

   .. code:: bash

      $ pipenv install openva-pipeline

   After this command, the output produced by Pipenv is shown in the following screenshot


   .. image:: Screenshots/pipenvOvaPL.png


#. Open a Pipenv shell load the openVA Pipeline package, download the ODK Briefcase, install
   the Transfer database, and run the Pipeline.  The final command exits out of Python.


   .. code:: bash

      $ pipenv shell


   (Python commands...)

   .. code:: python

      >>> import openva_pipeline as ovaPL
      >>> ovaPL.downloadBriefcase()
      >>> ovaPL.createTransferDB("Pipeline.db", ".", "enilepiP")
      >>> ovaPL.runPipeline("Pipeline.db", ".", "enilepiP")
      >>> quit()


   Here are a few more details about each Python command...

   #. ``import openva_pipeline as ovaPL`` -- load the openVA Pipeline package
      and use the shortcut name ovaPL.  To access the tools inside the openVA
      Pipeline package, use the nickname, followed by a ``.``, and then the name
      of the function or class.

   #. ``ovaPL.downloadBriefcase()`` --  call the function that downloads the
      `ODK Briefcase app <https://github.com/opendatakit/briefcase/releases>`_
      (version 12.2)
      from the `ODK GitHub page <https://github.com/opendatakit/briefcase>`_.
      The app will be downloaded to the current working directory.

   #. ``ovaPL.createTransferDB("Pipeline.db", ".", "enilepiP")`` -- create the
      SQLite database that contains the configuration settings for the Pipeline,
      stores VA data and results, and includes an event/error log.  The arguments
      are: (1) the name of the Transfer DB file ("Pipeline.db"); (2) the path
      to the directory where the DB file will be created; and (3) the
      key for encrypting the DB.

   #. ``ovaPL.runPipeline("Pipeline.db", ".", "enilepiP")``  -- run each
      step of the Pipeline: download records from ODK Aggregate; run openVA to
      assign causes of death; store the results in the Transfer DB; and post the
      VA data and assigned causes of death to a DHIS2 server (with the VA Program
      installed).  The arguments are (again): (1) the name of the Transfer DB
      file ("Pipeline.db"); (2) the path to the working directory for the Pipeline
      (the ODK Briefcase file and the Transfer DB need to be located in this
      directory); and (3) the key for encrypting the DB.

   #. ``quit()`` -- exit out of Python.


   A demonstration of these commands is shown in the following screenshot.


   .. image:: Screenshots/runPipeline.png



**Working with APIs for each Step**
-----------------------------------

*Transfer Database*
~~~~~~~~~~~~~~~~~~~

*ODK Aggregate & Briefcase*
~~~~~~~~~~~~~~~~~~~~~~~~~~~

*OpenVA*
~~~~~~~~

*DHIS2*
~~~~~~~

*Storing Results in the Transfer Database*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
