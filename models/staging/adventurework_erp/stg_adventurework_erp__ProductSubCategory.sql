-- models/staging/stg_adventurework_erp__ProductSubcategory.sql

with

source as (

    select * from {{ source('adventurework_erp', 'PRODUCTSUBCATEGORY') }}

),

productsubcategory as (

    select
        cast(productsubcategoryid as int) as pk_productsubcategoryid,
        cast(productcategoryid as int) as fk_productcategoryid,
        cast(name as varchar) as productsubcategory_name,
        cast(rowguid as varchar) as rowguid,
        cast(modifieddate as date) as modifieddate

    from source

)

select * from productsubcategory
