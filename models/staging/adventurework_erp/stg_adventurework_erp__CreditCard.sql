with 

source as (

    select * from {{ source('adventurework_erp', 'CreditCard') }}

),

renamed as (

    select
        creditcardid,
        cardtype,
        cardnumber,
        expmonth,
        expyear,
        modifieddate

    from source

)

select * from renamed
