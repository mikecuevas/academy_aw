with

source as (

    select * from {{ source('adventurework_erp', 'BusinessEntityAddress') }}

),

businessentityaddress as (

    select
        cast(businessentityid as int) as fk_businessentityid
        ,cast(addressid as int) as fk_addressid
        ,cast(addresstypeid as int) as fk_addresstypeid

    from source

)

select * from businessentityaddress
