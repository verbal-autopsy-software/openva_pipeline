Documentation for classes, functions, and methods
=================================================

.. module:: runPipeline
.. module:: pipeline
.. module:: transferDB
.. module:: odk
.. module:: openVA
.. module:: dhis

Main Interface
--------------

The OpenVA Pipeline is run using the following function

.. autofunction:: runPipeline.runPipeline


API for Transfer Database
-------------------------
.. autoclass:: transferDB.TransferDB
   :inherited-members:

API for ODK Briefcase
---------------------
.. autoclass:: odk.ODK
   :inherited-members:

API for OpenVA
--------------
.. autoclass:: openVA.OpenVA
   :inherited-members:

API for DHIS2
-------------
.. autoclass:: dhis.DHIS
   :inherited-members:
.. autoclass:: dhis.API
   :inherited-members:
.. autoclass:: dhis.VerbalAutopsyEvent
   :inherited-members:
.. autofunction:: create_db
.. autofunction:: getCODCode
.. autofunction:: findKeyValue

Excetptions
-----------
.. autoexception:: transferDB.PipelineError
.. autoexception:: transferDB.DatabaseConnectionError
.. autoexception:: transferDB.PipelineConfigureationError
.. autoexception:: transferDB.ODKConfigurationError
.. autoexception:: transferDB.OpenVAConfigurationError
.. autoexception:: transferDB.DHISConfigurationError
.. autoexception:: odk.ODKError
.. autoexception:: openVA.OpenVAError
.. autoexception:: openVA.SmartVAError
.. autoexception:: dhis.DHISConfigurationError
