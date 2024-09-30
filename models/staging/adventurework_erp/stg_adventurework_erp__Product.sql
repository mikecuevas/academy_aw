with 

source as (

    select * from {{ source('adventurework_erp', 'Product') }}

),

product as (
    select
        cast(productid as int) as pk_productid
        , cast(name as varchar) as name
        , cast(productnumber as varchar) as productnumber
        , cast(makeflag as boolean) as makeflag
        , cast(finishedgoodsflag as boolean) as finishedgoodsflag
        , cast(color as varchar) as color
        , cast(standardcost as decimal(19,4)) as standardcost
        , cast(listprice as decimal(19,4)) as listprice
        , cast(size as varchar) as size
        , cast(sizeunitmeasurecode as varchar) as sizeunitmeasurecode
        , cast(weight as decimal(19,4)) as weight
        , cast(weightunitmeasurecode as varchar) as weightunitmeasurecode
        , cast(daystomanufacture as int) as daystomanufacture
        , cast(productsubcategoryid as int) as fk_productsubcategoryid
        , cast(productmodelid as int) as fk_productmodelid
    from source
)

select * from product
