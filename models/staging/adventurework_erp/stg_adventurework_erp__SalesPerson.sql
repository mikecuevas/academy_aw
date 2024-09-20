with 

source as (

    select * from {{ source('adventurework_erp', 'SalesPerson') }}

),

salesperson as (

    select
        cast(businessentityid as int) as pk_businessentityid
        ,cast(territoryid as int) as fk_territoryid
        ,cast(salesquota as float) as salesquota
        ,cast(bonus as float) as bonus
        ,cast(commissionpct as float) as commissionpct
        ,cast(salesytd as float) as salesytd
        ,cast(saleslastyear as float) as saleslastyear
        ,cast(rowguid as varchar) as rowguid
        ,cast(modifieddate as date) as modifieddate

    from source

)

select * from salesperson
