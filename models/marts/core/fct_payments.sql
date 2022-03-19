{{
    config(
        materialized='incremental',
        unique_key='payment_id'
    )
}}


with payments as (

    select * from {{ ref('stg_payments') }}
    {% if is_incremental() %}
      where last_modified_date >= '2018-03-01'
    {% endif %}  
),

final as (

    select
        {{ dbt_utils.surrogate_key(['payments.payment_id']) }} payment_key,
        payments.payment_id,
        {{ dbt_utils.surrogate_key(['payments.order_id']) }} order_key,
        payments.payment_method,
        payments.amount,
        payments.last_modified_date

    from payments

)

select * from final
