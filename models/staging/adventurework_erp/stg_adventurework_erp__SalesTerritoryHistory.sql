with 

source as (

    select * from {{ source('adventurework_erp', 'SalesTerritoryHistory') }}

),

salesterritoryhistory as (

    select
        cast(businessentityid as int) as pk_businessentityid
        ,cast(territoryid as int) as fk_territoryid
        ,cast(startdate as date) as startdate
        ,cast(enddate as date) as enddate

    from source

)

select * from salesterritoryhistory
