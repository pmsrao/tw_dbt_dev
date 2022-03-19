{{
    config(
        materialized='view'
    )
}}

with orders as (

    select * from {{ ref('fct_orders') }}
),

payments as (

    select * from {{ ref('fct_payments') }}
),

customers as (
    select * from {{ ref('dim_customers') }}
),

final as (

    select
        customers.customer_id,
        concat_ws(customers.first_name, ' ', customers.last_name) as customer_name,
        min(orders.order_date) as first_order,
        max(orders.order_date) as most_recent_order,
        count(orders.order_id) as number_of_orders,
        sum(payments.amount) as total_payment_amount
    from customers
    left join orders 
        on (customers.customer_key = orders.customer_key)
    left join payments 
        on (payments.order_key = orders.order_key)

    group by 1, 2

)

select * from final
