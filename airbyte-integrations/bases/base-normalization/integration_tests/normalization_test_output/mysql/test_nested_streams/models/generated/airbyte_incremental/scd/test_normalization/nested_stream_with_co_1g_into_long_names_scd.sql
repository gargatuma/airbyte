{{ config(
    unique_key = "_airbyte_unique_key_scd",
    schema = "test_normalization",
    post_hook = ["
                    {%
                    set final_table_relation = adapter.get_relation(
                            database=this.database,
                            schema=this.schema,
                            identifier='nested_stream_with_co__lting_into_long_names'
                        )
                    %}
                    {#
                    If the final table doesn't exist, then obviously we can't delete anything from it.
                    Also, after a reset, the final table is created without the _airbyte_unique_key column (this column is created during the first sync)
                    So skip this deletion if the column doesn't exist. (in this case, the table is guaranteed to be empty anyway)
                    #}
                    {%
                    if final_table_relation is not none and '_airbyte_unique_key' in adapter.get_columns_in_relation(final_table_relation)|map(attribute='name')
                    %}
                    -- Delete records which are no longer active:
                    -- This query is equivalent, but the left join version is more performant:
                    -- delete from final_table where unique_key in (
                    --     select unique_key from scd_table where 1 = 1 <incremental_clause(normalized_at, final_table)>
                    -- ) and unique_key not in (
                    --     select unique_key from scd_table where active_row = 1 <incremental_clause(normalized_at, final_table)>
                    -- )
                    -- We're incremental against normalized_at rather than emitted_at because we need to fetch the SCD
                    -- entries that were _updated_ recently. This is because a deleted record will have an SCD record
                    -- which was emitted a long time ago, but recently re-normalized to have active_row = 0.
                    delete from {{ final_table_relation }} where {{ final_table_relation }}._airbyte_unique_key in (
                        select inactive_counts.unique_key
                        from (
                            select _airbyte_unique_key as unique_key, count(_airbyte_unique_key) as inactive_count
                            from {{ this }}
                            where _airbyte_active_row = 0 {{ incremental_clause('_airbyte_normalized_at', this.schema + '.' + adapter.quote('nested_stream_with_co__lting_into_long_names')) }}
                            group by _airbyte_unique_key
                        ) inactive_counts left join (
                            select _airbyte_unique_key as unique_key, count(_airbyte_unique_key) as active_count
                            from {{ this }}
                            where _airbyte_active_row = 1 {{ incremental_clause('_airbyte_normalized_at', this.schema + '.' + adapter.quote('nested_stream_with_co__lting_into_long_names')) }}
                            group by _airbyte_unique_key
                        ) active_counts on inactive_counts.unique_key = active_counts.unique_key
                        where active_count is null or active_count = 0
                    )
                    {% else %}
                    -- We have to have a non-empty query, so just do a noop delete
                    delete from {{ this }} where 1=0
                    {% endif %}
                    ","drop view _airbyte_test_normalization.nested_stream_with_co_1g_into_long_names_stg"],
    tags = [ "top-level" ]
) }}
-- depends_on: ref('nested_stream_with_co_1g_into_long_names_stg')
with
{% if is_incremental() %}
new_data as (
    -- retrieve incremental "new" data
    select
        *
    from {{ ref('nested_stream_with_co_1g_into_long_names_stg')  }}
    -- nested_stream_with_co__lting_into_long_names from {{ source('test_normalization', '_airbyte_raw_nested_s__lting_into_long_names') }}
    where 1 = 1
    {{ incremental_clause('_airbyte_emitted_at', this) }}
),
new_data_ids as (
    -- build a subset of _airbyte_unique_key from rows that are new
    select distinct
        {{ dbt_utils.surrogate_key([
            'id',
        ]) }} as _airbyte_unique_key
    from new_data
),
empty_new_data as (
    -- build an empty table to only keep the table's column types
    select * from new_data where 1 = 0
),
previous_active_scd_data as (
    -- retrieve "incomplete old" data that needs to be updated with an end date because of new changes
    select
        {{ star_intersect(ref('nested_stream_with_co_1g_into_long_names_stg'), this, from_alias='inc_data', intersect_alias='this_data') }}
    from {{ this }} as this_data
    -- make a join with new_data using primary key to filter active data that need to be updated only
    join new_data_ids on this_data._airbyte_unique_key = new_data_ids._airbyte_unique_key
    -- force left join to NULL values (we just need to transfer column types only for the star_intersect macro on schema changes)
    left join empty_new_data as inc_data on this_data._airbyte_ab_id = inc_data._airbyte_ab_id
    where _airbyte_active_row = 1
),
input_data as (
    select {{ dbt_utils.star(ref('nested_stream_with_co_1g_into_long_names_stg')) }} from new_data
    union all
    select {{ dbt_utils.star(ref('nested_stream_with_co_1g_into_long_names_stg')) }} from previous_active_scd_data
),
{% else %}
input_data as (
    select *
    from {{ ref('nested_stream_with_co_1g_into_long_names_stg')  }}
    -- nested_stream_with_co__lting_into_long_names from {{ source('test_normalization', '_airbyte_raw_nested_s__lting_into_long_names') }}
),
{% endif %}
scd_data as (
    -- SQL model to build a Type 2 Slowly Changing Dimension (SCD) table for each record identified by their primary key
    select
      {{ dbt_utils.surrogate_key([
      'id',
      ]) }} as _airbyte_unique_key,
      id,
      {{ adapter.quote('date') }},
      {{ adapter.quote('partition') }},
      {{ adapter.quote('date') }} as _airbyte_start_at,
      lag({{ adapter.quote('date') }}) over (
        partition by id
        order by
            {{ adapter.quote('date') }} is null asc,
            {{ adapter.quote('date') }} desc,
            _airbyte_emitted_at desc
      ) as _airbyte_end_at,
      case when row_number() over (
        partition by id
        order by
            {{ adapter.quote('date') }} is null asc,
            {{ adapter.quote('date') }} desc,
            _airbyte_emitted_at desc
      ) = 1 then 1 else 0 end as _airbyte_active_row,
      _airbyte_ab_id,
      _airbyte_emitted_at,
      _airbyte_nested_strea__nto_long_names_hashid
    from input_data
),
dedup_data as (
    select
        -- we need to ensure de-duplicated rows for merge/update queries
        -- additionally, we generate a unique key for the scd table
        row_number() over (
            partition by
                _airbyte_unique_key,
                _airbyte_start_at,
                _airbyte_emitted_at
            order by _airbyte_active_row desc, _airbyte_ab_id
        ) as _airbyte_row_num,
        {{ dbt_utils.surrogate_key([
          '_airbyte_unique_key',
          '_airbyte_start_at',
          '_airbyte_emitted_at'
        ]) }} as _airbyte_unique_key_scd,
        scd_data.*
    from scd_data
)
select
    _airbyte_unique_key,
    _airbyte_unique_key_scd,
    id,
    {{ adapter.quote('date') }},
    {{ adapter.quote('partition') }},
    _airbyte_start_at,
    _airbyte_end_at,
    _airbyte_active_row,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_nested_strea__nto_long_names_hashid
from dedup_data where _airbyte_row_num = 1

