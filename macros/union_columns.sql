{% macro union_columns(table_name, id_column, columns_to_union) %}
  {% for column in columns_to_union %}
    SELECT
      {{ id_column }},
      {{ column }} as unified_column
    FROM {{ table_name }}
    WHERE {{ column }} IS NOT NULL
    {% if not loop.last %}
    UNION ALL
    {% endif %}
  {% endfor %}
{% endmacro %}