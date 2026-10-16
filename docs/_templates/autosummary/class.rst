{%- set is_root = basename == project.lower() -%}
{%- set module_footer = ["base", "exception"] -%}
{#- words to fully capitalise or keep lowercase in titles; all other words are title cased -#}
{%- set module_caps = ["cli"] -%}
{%- set module_lower = ["dbt"] -%}

{%- macro formatname(name) -%}
    {%- if name.endswith(".exception") -%}
        {%- set name = "exceptions"-%}
    {%- else -%}
        {%- set name = name.replace(project + ".", "").replace("_", " ").split(".") | last -%}
    {%- endif -%}

    {%- for word in name.split(" ") -%}
        {%- if word | lower in module_caps -%}
            {{- word | upper -}}
        {%- elif word | lower in module_lower -%}
            {{- word | lower -}}
        {%- else -%}
            {{- word | title -}}
        {%- endif -%}
        {%- if not loop.last %} {% endif -%}
    {%- endfor -%}
{%- endmacro -%}

{{ formatname(fullname) | escape | underline}}

.. currentmodule:: {{ module }}

.. autoclass:: {{ objname }}

   {%- block methods -%}
   .. automethod:: __init__

   {%- if methods -%}
   .. rubric:: {{ _('Methods') }}

   .. autosummary::
   {%- for item in methods -%}
      ~{{ name }}.{{ item }}
   {%- endfor %}
   {%- endif -%}
   {%- endblock -%}

   {%- block attributes -%}
   {%- if attributes -%}
   .. rubric:: {{ _('Attributes') }}

   .. autosummary::
   {%- for item in attributes -%}
      ~{{ name }}.{{ item }}
   {%- endfor %}
   {%- endif -%}
   {%- endblock -%}
