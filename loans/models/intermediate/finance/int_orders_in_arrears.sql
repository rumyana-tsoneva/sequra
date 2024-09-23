

with data as (
    select
        o.order_id
        ,o.shopper_id
        ,s.age
        ,o.order_date
        ,o.order_month
        ,o.product_id
        ,o.merchant_id
        ,d.default_type
        ,o.delayed_period
        ,o.current_order_value                 loan_in_arrears
        ,o.overdue_principal                   principal_in_arrears
        ,o.overdue_fees                        fees_in_arrears
        ,o.overdue_principal + o.overdue_fees  debt
    from {{ ref('stg_orders') }} o
    
    left join {{ ref('stg_shoppers') }} s
        on o.shopper_id = s.shopper_id

    left join {{ ref('stg_default_order_type') }} d
        on o.order_id = d.order_id
    
    where o.is_in_default = true

)

select * from data