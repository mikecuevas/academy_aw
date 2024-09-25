with

source as (

    select * from {{ source('adventurework_erp', 'BusinessEntityAddress') }}

),

businessentityaddress as (

    select
        cast(businessentityid as int) as fk_businessentityid,
        cast(addressid as int) as fk_addressid,
        cast(addresstypeid as int) as fk_addresstypeid,
        cast(rowguid as varchar) as rowguid,
        cast(modifieddate as date) as modifieddate

    from source

)

select * from businessentityaddress
