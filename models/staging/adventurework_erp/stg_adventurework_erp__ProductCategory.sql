with 

source as (

    select * from {{ source('adventurework_erp', 'ProductCategory') }}

),

productcategory as (

    select
        cast(productcategoryid as int) as pk_productcategoryid
        ,cast(name as varchar) as productcategory_name
        ,cast(rowguid as varchar) as rowguid
        ,cast(modifieddate as date) as modifieddate

    from source

)

select * from productcategory
