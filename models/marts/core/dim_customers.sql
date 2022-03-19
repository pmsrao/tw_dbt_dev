{{
    config(
        materialized='incremental',
        unique_key='customer_id'
    )
}}

with customers as (

    select * from {{ ref('stg_customers') }}
    {% if is_incremental() %}
      where last_modified_date >= '2018-03-01'
    {% endif %}    

),

final as (

    select
        {{ dbt_utils.surrogate_key(['customers.customer_id']) }} customer_key,
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customers.last_modified_date

    from customers

)

select * from final