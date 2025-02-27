{%- macro edr_cast_as_timestamp(timestamp_field) -%}
    cast({{ timestamp_field }} as {{ elementary.edr_type_timestamp() }})
{%- endmacro -%}

{# Custom macro used only to fix string cast to timestamp in Jinja context #}
{%- macro edr_cast_string_to_timestamp(timestamp_field) -%}
    '{{ timestamp_field }}'::TIMESTAMP
{%- endmacro -%}

{%- macro edr_cast_as_float(column) -%}
    cast({{ column }} as {{ elementary.edr_type_float() }})
{%- endmacro -%}

{%- macro edr_cast_as_numeric(column) -%}
    cast({{ column }} as {{ elementary.edr_type_numeric() }})
{%- endmacro -%}

{%- macro edr_cast_as_int(column) -%}
    cast({{ column }} as {{ elementary.edr_type_int() }})
{%- endmacro -%}

{%- macro edr_cast_as_string(column) -%}
    cast({{ column }} as {{ elementary.edr_type_string() }})
{%- endmacro -%}

{%- macro edr_cast_as_long_string(column) -%}
    cast({{ column }} as {{ elementary.edr_type_long_string() }})
{%- endmacro -%}

{%- macro edr_cast_as_bool(column) -%}
    cast({{ column }} as {{ elementary.edr_type_bool() }})
{%- endmacro -%}

{%- macro const_as_string(string) -%}
    cast('{{ string }}' as {{ elementary.edr_type_string() }})
{%- endmacro -%}

{%- macro edr_cast_as_date(timestamp_field) -%}
    {{ return(adapter.dispatch('edr_cast_as_date', 'elementary')(timestamp_field)) }}
{%- endmacro -%}

{%- macro default__edr_cast_as_date(timestamp_field) -%}
    cast({{ timestamp_field }} as {{ elementary.edr_type_date() }})
{%- endmacro -%}

{# Bigquery (for some reason that is beyond me) can't cast a string as date if it's in timestamp format #}
{%- macro bigquery__edr_cast_as_date(timestamp_field) -%}
    cast({{ elementary.edr_cast_as_timestamp(timestamp_field) }} as {{ elementary.edr_type_date() }})
{%- endmacro -%}

{%- macro const_as_text(string) -%}
    {{ return(adapter.dispatch('const_as_text', 'elementary')(string)) }}
{%- endmacro -%}

{%- macro default__const_as_text(string) -%}
    {{ elementary.const_as_string(string) }}
{%- endmacro -%}

{%- macro redshift__const_as_text(string) -%}
    '{{ string }}'::text
{%- endmacro -%}
