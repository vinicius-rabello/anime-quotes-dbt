{%- set tables = (
    dbt_utils.get_column_values(
        source('anime_quotes', 'v_tables_info'), 
        'table_name'
    )
) -%}

with final as (
    {% for table_name in tables %}
        select
            '{{ table_name }}' as table_name,
            exists (
                select 1
                from {{ source('anime_quotes', table_name) }}
                where name <> 'Unknown' and name <> 'NTP'
            ) as has_known_character
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

select * from final