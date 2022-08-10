{{ objname | escape | underline}}

.. **module**

.. automodule:: {{ fullname }}

   {% block functions %}
   {% if functions %}
   .. rubric:: Functions

   .. autosummary::
      :toctree: {{name}}/
      :template: autosummary_theme/function.rst       
   {% for item in functions %}
      {{ item }}
   {%- endfor %}
   {% endif %}
   {% endblock %}

   {% block classes %}
   {% if classes %}
   .. rubric:: Classes

   .. autosummary::
      :toctree: {{name}}/
      :template: autosummary_theme/class.rst    
   {% for item in classes %}
      {{ item }}
   {%- endfor %}
   {% endif %}
   {% endblock %}

   {% block attributes %}
   {% if attributes %}
   .. rubric:: Attributes

   .. autosummary::
      :toctree: {{name}}/
      :template: autosummary_theme/attribute.rst 
   {% for item in all_attributes %}
   {%- if not item.startswith('_') %}
      ~{{ item }}
   {%- endif -%}
   {%- endfor %}
   {% endif %}
   {% endblock %}

   {% block submodules %}
   {% if modules %}
   .. rubric:: Submodules

   .. autosummary::
      :toctree: {{name}}/
      :template: autosummary_theme/module.rst
   {% for item in modules %}
      ~{{ item }}
   {%- endfor %}

   {% endif %}
   {% endblock %}
