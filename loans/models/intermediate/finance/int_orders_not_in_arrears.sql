

with data as (
    select
        o.order_id
        ,o.shopper_id
        ,s.age
        ,o.order_date
        ,o.order_month
        ,o.product_id
        ,o.merchant_id
        ,o.delayed_period
        ,o.current_order_value                 loan_not_in_arrears
    
    from {{ ref('stg_orders') }} o
    
    left join {{ ref('stg_shoppers') }} s
        on o.shopper_id = s.shopper_id

    where o.is_in_default = false

)

select * from data