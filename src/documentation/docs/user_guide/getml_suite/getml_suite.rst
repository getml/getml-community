.. _getml_suite:

The getML suite
===============

The getML software consists of three fundamental components:

.. toctree::
    :maxdepth: 1

    Engine <engine>
    Monitor <monitor> 
    Python API <python_api>

The **getML engine** is written in C++ and is
the heart of the getML suite. It holds all data, is responsible for
the feature engineering and the machine learning (ML) part, and does
all the heavy lifting. 

You can control the engine using the **getML Python API**.  The API
provides handlers to the objects in the engine and all functionalities necessary
to do an end-to-end data science project. 

To help you explore the various data sets and
ML models built during your analysis, we provide the **getML monitor**.
The monitor is written in Go. In addition to visualization, 
it lets you
handle the login and the account management for the getML suite.

To get started with the getML, head over to the :ref:`installation`
instructions.
