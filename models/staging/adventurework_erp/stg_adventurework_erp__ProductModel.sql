with 

source as (

    select * from {{ source('adventurework_erp', 'ProductModel') }}

),

productmodel as (

    select
        cast(productmodelid as int) as pk_productmodelid
        ,cast(name as varchar) as productmodel_name
        ,cast(catalogdescription as varchar) as catalogdescription
        ,cast(instructions as varchar) as instructions
        ,cast(rowguid as varchar) as rowguid
        ,cast(modifieddate as date) as modifieddate

    from source

)

select * from productmodel
