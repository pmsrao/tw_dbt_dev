with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ ref('raw_customers') }}

),

renamed as (

    select
        id as customer_id,
        cast(first_name as varchar(15) ),
        cast(last_name as varchar(15) ),
        cast (email as varchar(25) ),
        last_modified_date

    from source

)

select * from renamed
