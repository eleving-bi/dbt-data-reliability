{% macro get_elementary_test_table(test_name, table_type) %}
    {% if execute %}
        {% if test_name | length > 80 %}
            {# {{ log("Truncating 'test_name' from '" ~ test_name ~ "' to 80 characters because the full 'test_name' with additional suffix is too long for Vertica (max 128 characters)", "debug") }} #}
            {% set test_name = test_name | truncate(80, True, '') %}
        {% endif %}
        {% set test_entry = elementary.get_cache("temp_test_table_relations_map").setdefault(test_name, {}) %}
        {% do return(test_entry.get(table_type)) %}
    {% endif %}
    {% do return(none) %}
{% endmacro %}
