with 

source as (

    select * from {{ source('adventurework_erp', 'Customer') }}

),

customer as (

    select
        cast(customerid as int) as pk_customerid
        ,cast(personid as int) as fk_personid
        ,cast(storeid as int) as fk_storeid
        ,cast(territoryid as int) as fk_territoryid
        ,cast(rowguid as varchar) as rowguid
        ,cast(modifieddate as date) as modifieddate

    from source

)

select * from customer
