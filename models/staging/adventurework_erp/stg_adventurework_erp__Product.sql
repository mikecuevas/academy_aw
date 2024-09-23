with 

source as (

    select * from {{ source('adventurework_erp', 'Product') }}

),

product as (

    select
        cast(productid as int) as pk_productid
        ,cast(productsubcategoryid as int) as fk_productsubcategoryid
        ,cast(productmodelid as int) as fk_productmodelid
        ,cast(name as varchar) as product_name
        ,cast(productnumber as varchar) as productnumber
        ,cast(makeflag as boolean) as makeflag
        ,cast(finishedgoodsflag as boolean) as finishedgoodsflag
        ,cast(color as varchar) as color
        ,cast(safetystocklevel as int) as safetystocklevel
        ,cast(reorderpoint as int) as reorderpoint
        ,cast(standardcost as float) as standardcost
        ,cast(listprice as float) as listprice
        ,cast(size as varchar) as size
        ,cast(sizeunitmeasurecode as varchar) as sizeunitmeasurecode
        ,cast(weightunitmeasurecode as varchar) as weightunitmeasurecode
        ,cast(weight as float) as product_weight
        ,cast(daystomanufacture as int) as daystomanufacture
        ,cast(productline as varchar) as productline
        ,cast(class as varchar) as product_class
        ,cast(style as varchar) as product_style
        ,cast(sellstartdate as date) as sellstartdate
        ,cast(sellenddate as date) as sellenddate
        ,cast(discontinueddate as varchar) as discontinueddate
        ,cast(rowguid as varchar) as rowguid
        ,cast(modifieddate as date) as modifieddate

    from source

)

select * from product
