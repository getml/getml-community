=======================
getML Documentation
=======================

Welcome to the getML technical documentation. This document is written for data
scientists who want to use the getML software suite for their projects. For
general information about getML visit https://getml.com. For a collection of
demo notebooks, visit  https://github.com/getml/getml-demo. You can also `contact
us <https://getml.com/contact/lets-talk>`_ for any questions or inquiries.

____

.. note::
   Some components of getML have been open sourced as part of **getML community edition**. 
   You may have a look at 
   `community vs enterprise edition table <https://docs.getml.com/latest/home/getting_started/getting_started.html#community-vs-enterprise-edition>`_ to see the highlights of both the editions. 



getML in one minute 
-------------------

getML is an innovative tool for the end-to-end automation of data
science projects. It covers everything from convenient data loading procedures 
to the deployment of trained models. 

Most notably, getML includes advanced algorithms for
**automated feature engineering** (feature learning) on relational data and time
series. Feature engineering on relational data is defined as the creation of a 
flat table by merging and aggregating data. It is sometimes also referred to
as **data wrangling**. Feature engineering is necessary if your data is distributed
over more than one data table.

Automated feature engineering

* Saves up to 90% of the time spent on a data science project
* Increases the prediction accuracy over manual feature engineering 

Andrew Ng, Professor at Stanford
University and Co-founder of Google Brain described manual feature engineering as follows:

    *Coming up with features is difficult, time-consuming, requires expert
    knowledge. "Applied machine learning" is basically feature engineering.*

The main purpose of getML is to automate this *"difficult, time-consuming"* process as much
as possible.

getML comes with a high-performance **engine** written in C++ and an intuitive
**Python API**. Completing a data science project with getML consists of seven
simple steps.

.. code-block:: python
				
   import getml
   
   getml.engine.launch()
   getml.engine.set_project('one_minute_to_getml')

1. Load the data into the engine

.. code-block:: python

   population = getml.data.DataFrame.from_csv('data_population.csv',
				name='population_table')
   peripheral = getml.data.DataFrame.from_csv('data_peripheral.csv',
				name='peripheral_table')
				
2. Annotate the data
   
.. code-block:: python
				
   population.set_role('target', getml.data.role.target)
   population.set_role('join_key', getml.data.role.join_key)
   ...

3. Define the data model

.. code-block:: python   

   dm = getml.data.DataModel(population.to_placeholder("POPULATION"))
   dm.add(peripheral.to_placeholder("PERIPHERAL"))
   dm.POPULATION.join(
       dm.PERIPHERAL,
       on="join_key",
   )

4. Train the feature learning algorithm and the predictor

.. code-block:: python   
    
   pipe = getml.pipeline.Pipeline(
        data_model=dm,
        feature_learners=getml.feature_learning.FastProp()
        predictors=getml.predictors.LinearRegression()
   )

   pipe.fit(
        population=population,
        peripheral=[peripheral]
   )

5. Evaluate

.. code-block:: python   

   pipe.score(
        population=population_unseen,
        peripheral=[peripheral_unseen]
   )

6. Predict   

.. code-block:: python   

    pipe.predict(
        population=population_unseen,
        peripheral=[peripheral_unseen]
    )

7. Deploy

.. code-block:: python   

    # Allow the pipeline to respond to HTTP requests
    pipe.deploy(True)

Check out the rest of this documentation to find out how getML achieves top
performance on real-world data science projects with many tables and complex
data schemes.

____

How to use this guide
---------------------

If you want to get started with getML right away, we recommend to follow the
:ref:`installation instructions <installation>` and then go through the
:ref:`getting started <getting_started>` guide. 

If you are looking for more detailed information, other sections of this
documentation are more suitable. There are three major parts: 


`Tutorials <https://github.com/getml/getml-demo>`__

  The tutorials section contains examples of how to use getML in 
  real-world projects. All tutorials are based on public data sets 
  so that
  you can follow along. If you are looking for an intuitive access to
  getML, the tutorials section is the right place to go. Also, the
  code examples are explicitly intended to be used as a template for
  your own projects.  

:ref:`User guide <user_guide>`   

  The user guide explains all conceptional details behind getML in
  depth. It can serve as a reference guide for experienced users but it's also
  suitable for first day users who want to get a deeper understanding
  of how getML works. Each chapter in the
  user guide represents one step of a typical data science project.

:ref:`API documentation <python_api>`

  The API documentation covers everything related to the Python
  interface to the getML engine. Each module comes with a dedicated
  section that contains concrete code examples.

You can also check out our other resources

* `getML homepage <https://getml.com>`_

.. toctree::
   :caption: Home
   :hidden:
   
   Installation <home/installation/installation>
   How to use this guide <home/how_to_use_this_guide>
   Getting started <home/getting_started/getting_started>
   Support <home/support/support>


.. toctree::
   :caption: User Guide
   :titlesonly:
   :hidden:   

   Tutorials <https://github.com/getml/getml-demo>
   getML suite <user_guide/getml_suite/getml_suite>
   Managing projects <user_guide/project_management/project_management>
   Importing data <user_guide/importing_data/importing_data>
   Annotating data <user_guide/annotating_data/annotating_data>
   Data model <user_guide/data_model/data_model>
   Preprocessing <user_guide/preprocessing/preprocessing>
   Feature engineering <user_guide/feature_engineering/feature_engineering>
   Predicting <user_guide/predicting/predicting>
   Hyperparameter optimization <user_guide/hyperopt/hyperopt>
   Deployment <user_guide/deployment/deployment>


.. toctree::
   :caption: API Documentation
   :titlesonly:
   :hidden:   

   Python API <api_reference/index>
   Command line interface <api_reference/cli>


.. toctree::
   :caption: Integration
   :titlesonly:
   :hidden:

   Fast API <integration/fastapi/fastapi>


.. toctree::
   :hidden:
   
   about
