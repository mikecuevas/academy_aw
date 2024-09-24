with 

source as (

    select * from {{ source('adventurework_erp', 'ShipMethod') }}

),

shipmethod as (

    select
        cast(shipmethodid as int) as shipmethodid,
        cast(name as varchar) as name,
        cast(shipbase as decimal(15,2)) as shipbase,
        cast(shiprate as decimal(15,2)) as shiprate,
        cast(rowguid as varchar) as rowguid,
        cast(modifieddate as date) as modifieddate

    from source

)

select * from shipmethod
