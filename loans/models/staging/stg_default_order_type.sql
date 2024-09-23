with source as (
    select 
        default_type_id default_type
        ,order_id
     from {{ source('raw_data', 'default_order_type') }}
)


select * from source
