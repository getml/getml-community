{% extends "./module.rst" %}

{% block members %}
{% if members %}
.. rubric:: Variables

    .. autosummary::
        :toctree: {{name}}/
    {% for item in members %}
    {%- if not item.startswith('_') %}
        {{ item }}
    {%- endif -%}
    {%- endfor %}

{% endif %}
{% endblock %}