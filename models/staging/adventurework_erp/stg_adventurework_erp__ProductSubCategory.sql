-- models/staging/stg_adventurework_erp__ProductSubcategory.sql

with

source as (

    select * from {{ source('adventurework_erp', 'ProductSubCategory') }}

),

productsubcategory as (

    select
        cast(productsubcategoryid as int) as pk_productsubcategoryid
        ,cast(productcategoryid as int) as fk_productcategoryid
        ,cast(name as varchar) as productsubcategory_name

    from source

)

select * from productsubcategory
