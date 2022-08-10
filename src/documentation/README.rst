**************************
Read the getML Docs Theme
**************************

Build locally
==============

Assuming you have installed the dependencies, you can run

bash build_local.sh 

to build the documentation locally.

Build Theme
============

.. code:: python

   # setup your venv & cd to package root
   pip install -r ./docs/requirements.txt
   yarn

   # to build on your own machine & start local dev server
   yarn build dev

   # open browser at http://localhost:1922

   # if issues with autosummary writer occur just delet the folder ./docs/api

   # ToDos:
   # [ ] docsearch switch magic muss in ./docs/_static/js/getml.js
   # [ ] when everything works move all getml module docs from ./_off/* 
   #     back into ./docs/api_reference/


For more information ask alex.
