{{ name | escape | underline}}

.. **attribute**

.. currentmodule:: {{ module }}

{% if name == "name" %}

.. data:: name

   Holds the name of the current project.

{% else %}

.. auto{{ objtype }}:: {{ objname }}
   :annotation:

{% if name in ["pipelines", "hyperopts"] %}

   Returns:
      An instance of :class:`getml.project.{{ objname | title }}`\ .

{% endif %}
{% if name == "data_frames" %}

   Returns:
      An instance of :class:`getml.project.DataFrames`\ .

{% endif %}
{% endif %}

