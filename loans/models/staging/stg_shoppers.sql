
with source as (
    select * from {{ source ('raw_data', 'dim_shoppers') }}
)


select * from source
