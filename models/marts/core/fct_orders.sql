{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

with orders as (

    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
      where last_modified_date >= '2018-03-01'
    {% endif %}  
),

final as (

    select
        {{ dbt_utils.surrogate_key(['orders.order_id']) }} order_key,
        orders.order_id,
        {{ dbt_utils.surrogate_key(['orders.customer_id']) }} customer_key,
        orders.order_date,
        orders.status,
        orders.last_modified_date

    from orders

)

select * from final
