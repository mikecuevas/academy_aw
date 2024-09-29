with 

source as (

    select * from {{ source('adventurework_erp', 'CreditCard') }}

),

credit_card as (

    select
        cast (creditcardid as int) as pk_creditcardid
        ,cast (cardtype as varchar) as cardtype

    from source

)

select * from credit_card
