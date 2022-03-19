with source as (
    
    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ ref('raw_payments') }}

),

renamed as (

    select
        id as payment_id,
        order_id,
        cast(payment_method as varchar(15)),

        --`amount` is currently stored in cents, so we convert it to dollars
        amount / 100 as amount, 
        last_modified_date

    from source

)

select * from renamed
