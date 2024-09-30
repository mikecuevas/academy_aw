with 

source as (

    select * from {{ source('adventurework_erp', 'ProductModel') }}

),

productmodel as (

    select
        cast(productmodelid as int) as pk_productmodelid
        , cast(name as varchar) as productmodel_name

    from source

)

select * from productmodel
