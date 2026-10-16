{%- set is_root = basename == project.lower() -%}
{%- set module_footer = ["base", "exception"] -%}
{%- set module_caps = [] -%}

{%- macro formatname(name) -%}
    {%- if name.endswith(".exception") -%}
        {%- set name = "exceptions"-%}
    {%- else -%}
        {%- set name = name.replace(project + ".", "").replace("_", " ").split(".") | last -%}
    {%- endif -%}

    {%- if name | lower in module_caps -%}
        {{- name | upper -}}
    {%- else -%}
        {{- name | title -}}
    {%- endif -%}
{%- endmacro -%}

{{ formatname(fullname) | escape | underline}}

{% if classes -%}
{#- list classes explicitly so packages include the classes they re-export via __all__ -#}
.. inheritance-diagram:: {% for item in classes %}{{ fullname }}.{{ item }} {% endfor %}
   :parts: 1
{%- endif %}

.. automodule:: {{ fullname }}

   {%- block attributes -%}
   {%- if attributes -%}
   .. rubric:: {{ _('Module Attributes') }}

   .. autosummary::
   {% for item in attributes %}
      {{ item }}
   {%- endfor -%}
   {%- endif -%}
   {% endblock %}

   {% block functions -%}
   {% if functions -%}
   .. rubric:: {{ _('Functions') }}
   .. autosummary::
   {% for item in functions %}
      {{ item }}
   {%- endfor -%}
   {%- endif -%}
   {% endblock %}

   {% block classes -%}
   {% if classes -%}
   .. rubric:: {{ _('Classes') }}
   .. autosummary::
   {% for item in classes %}
      {{ item }}
   {%- endfor -%}
   {%- endif -%}
   {% endblock %}

   {% block exceptions -%}
   {% if exceptions -%}
   .. rubric:: {{ _('Exceptions') }}
   .. autosummary::
   {% for item in exceptions %}
      {{ item }}
   {%- endfor -%}
   {%- endif -%}
   {%- endblock %}

{%- block modules -%}
{#- modules excludes submodules missing from a package's __all__, so filter all_modules for public ones -#}
{%- set public_modules = [] -%}
{%- for item in all_modules if not item.startswith("_") -%}
{%- set _ = public_modules.append(item) -%}
{%- endfor -%}
{%- if public_modules %}
.. rubric:: Modules

.. autosummary::
   :toctree:
   :recursive:
{% for item in public_modules %}
   {{ item }}
{% endfor %}
{% endif %}
{% endblock %}
