-- models/staging/stg_adventurework_erp__ProductSubcategory.sql

with

source as (

    select * from {{ source('adventurework_erp', 'PRODUCTSUBCATEGORY') }}

),

    productsubcategory as (
        select
          cast(productsubcategoryid as int) as pk_productsubcategoryid
          , cast(name as varchar) as productsub_name
          , cast(productcategoryid as int) as fk_productcategoryid
        from source
    )

select * from productsubcategory
