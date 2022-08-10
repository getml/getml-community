# Hard coded to my laptop sorry - complain if I should fix it, JK
# Not on my watch - PM


FILENAME="getting_started.rst"

jupyter-nbconvert --FilesWriter.build_directory=. ../../getml-examples/python/projects/getting_started/getting_started.ipynb --to rst --template ../../utilities/jupyter/getml_docs_template.tpl --output $FILENAME || exit 1

echo '.. Auto generated file. Do NOT edit

.. _getting_started:

' | cat - $FILENAME > temp && mv temp $FILENAME
