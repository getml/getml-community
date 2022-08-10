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

   population_placeholder = getml.data.Placeholder("POPULATION")
   peripheral_placeholder = getml.data.Placeholder("PERIPHERAL")
   population_placeholder.join(peripheral_placeholder, join_key="join_key")

4. Train the feature learning algorithm and the predictor

.. code-block:: python   
    
   pipe = getml.pipeline.Pipeline(
        population=population_placeholder,
        peripheral=[peripheral_placeholder],
        feature_learners=getml.feature_learning.MultirelModel(num_features=100)
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