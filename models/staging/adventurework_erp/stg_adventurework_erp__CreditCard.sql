with 

source as (

    select * from {{ source('adventurework_erp', 'CreditCard') }}

),

credit_card as (

    select
        cast (creditcardid as int) as pk_creditcardid
        ,cast (cardtype as varchar) as cardtype
        ,cast (cardnumber as varchar) as cardnumber
        ,cast (expmonth as int) as expmonth
        ,cast (expyear as int) as expyear
        ,cast (modifieddate as date) as modifieddate

    from source

)

select * from credit_card
