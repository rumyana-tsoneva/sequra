{{
    config(
        materialized='table',
        schema='marts'
    )
}}


with debt as (
    select
        age
        ,order_month
        ,product_id
        ,merchant_id
        ,default_type
        ,delayed_period
        ,sum(loan_in_arrears)       total_loan_in_arrears
        ,sum(principal_in_arrears)  total_principal_in_arrears
        ,sum(fees_in_arrears)       total_fees_in_arrears
        ,sum(debt)                  total_debt
    from {{ ref('int_orders_in_arrears') }}

    group by 1,2,3,4,5,6
),

loans as (
    select
        age
        ,order_month
        ,product_id
        ,merchant_id
        ,sum(loan_not_in_arrears)       total_loan_not_in_arrears

    from {{ ref('int_orders_not_in_arrears') }}

    group by 1,2,3,4
),

loans_default as (
    select 
        d.age                                                               shopper_age
        ,d.order_month                                                      month_year_order
        ,d.product_id                                                       product
        ,d.merchant_id                                                      merchant
        ,d.default_type                                                     default_type
        ,d.delayed_period                                                   delayed_period
        ,d.total_debt                                                       total_debt 
        ,d.total_loan_in_arrears                                            total_loan_in_arrears
        ,coalesce(l.total_loan_not_in_arrears, 0)                           total_loan_not_in_arrears
        ,d.total_loan_in_arrears + coalesce(l.total_loan_not_in_arrears, 0)              
                                                                            total_loans
        ,round(div0(d.total_debt, (d.total_loan_in_arrears + coalesce(l.total_loan_not_in_arrears, 0) )), 4) * 100 
                                                                            default_ratio -- total loans in arrears / total loans - based on current order value 
    from debt d
    left join loans l
        on d.age = l.age
        and d.order_month = l.order_month
        and d.product_id = l.product_id
        and d.merchant_id = l.merchant_id

    order by shopper_age, month_year_order, product, merchant
)


select * from loans_default