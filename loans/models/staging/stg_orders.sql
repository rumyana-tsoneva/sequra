{{ 
    config(materialized='view') 
}}

with source as (

    select
        order_id
        ,shopper_id
        ,order_date
        ,to_date(left(order_date,7), 'yyyy-MM') order_month
        ,product_id
        ,merchant_id
        ,is_in_default
        ,days_unbalanced
        ,case
            when days_unbalanced = 0 then '0'
            when days_unbalanced = 35 then '17 - 30' -- either create category here or count orders of this type twice
            when days_unbalanced <= 17 then '17'
            when days_unbalanced <= 30 then '30'
            when days_unbalanced <= 60 then '60'
            when days_unbalanced <= 90 then '90'
        end
        as delayed_period
        ,current_order_value
        ,overdue_principal
        ,overdue_fees
    from {{ source('raw_data', 'orders') }}

)

select *
from source
