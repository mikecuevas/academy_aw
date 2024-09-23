with 

source as (

    select * from {{ source('adventurework_erp', 'ShipMethod') }}

),

shipmethod as (

    select
        shipmethodid,
        name,
        shipbase,
        shiprate,
        rowguid,
        modifieddate

    from source

)

select * from shipmethod
