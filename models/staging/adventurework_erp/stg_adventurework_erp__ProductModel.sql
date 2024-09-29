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

    from source

)

select * from productmodel
