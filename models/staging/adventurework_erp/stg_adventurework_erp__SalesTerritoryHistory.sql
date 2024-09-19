with 

source as (

    select * from {{ source('adventurework_erp', 'SalesTerritoryHistory') }}

),

renamed as (

    select
        businessentityid,
        territoryid,
        startdate,
        enddate,
        rowguid,
        modifieddate

    from source

)

select * from renamed
