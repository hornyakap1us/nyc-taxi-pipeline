-- macros/generate_surrogate_key.sql
-- -----------------------------------
-- Simple surrogate key macro.
-- Usage: {{ dbt_utils_generate_surrogate_key(['col1', 'col2']) }}
--
-- In production you'd use the dbt_utils package for this
-- (add to packages.yml: dbt-labs/dbt_utils).
-- This inline version works without installing packages.

{% macro dbt_utils_generate_surrogate_key(field_list) %}
    to_hex(md5(concat(
        {% for field in field_list %}
            coalesce(cast({{ field }} as string), 'NULL')
            {% if not loop.last %}, '|', {% endif %}
        {% endfor %}
    )))
{% endmacro %}
