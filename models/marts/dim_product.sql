with
    stg_product as (
        select
            pk_productid
            ,fk_productsubcategoryid
            ,fk_productmodelid
            ,product_name
            ,productnumber
            ,makeflag
            ,finishedgoodsflag
            ,color
            ,safetystocklevel
            ,reorderpoint
            ,standardcost
            ,listprice
            ,size
            ,sizeunitmeasurecode
            ,weightunitmeasurecode
            ,product_weight
            ,daystomanufacture
            ,productline
            ,product_class
            ,product_style
            ,sellstartdate
            ,sellenddate
            ,discontinueddate

        from {{ ref("stg_adventurework_erp__Product") }}
    )
    , stg_productsubcategory as (
        select
            pk_productsubcategoryid
            ,fk_productcategoryid
            ,productsubcategory_name

        from {{ ref("stg_adventurework_erp__ProductSubCategory") }}
    )
    , stg_productcategory as (
        select
            pk_productcategoryid
            ,productcategory_name


        from {{ ref("stg_adventurework_erp__ProductCategory") }}
    )
    , stg_productmodel as (
        select
            pk_productmodelid
            ,productmodel_name
            ,catalogdescription
            ,instructions

        from {{ ref("stg_adventurework_erp__ProductModel") }}
    )

    , joined as (
        select
            prod.pk_productid
            ,prod.product_name
            ,prod.productnumber
            ,prod.makeflag
            ,prod.finishedgoodsflag
            ,prod.color
            ,prod.safetystocklevel
            ,prod.reorderpoint,
            ,prod.standardcost
            ,prod.listprice
            ,prod.size
            ,prod.sizeunitmeasurecode
            ,prod.weightunitmeasurecode
            ,prod.product_weight
            ,prod.daystomanufacture
            ,prod.productline
            ,prod.product_class
            ,prod.product_style
            ,prod.sellstartdate
            ,prod.sellenddate
            ,prod.discontinueddate
            ,pm.productmodel_name
            ,pm.catalogdescription
            ,pm.instructions
            ,psubcat.productsubcategory_name
            ,pcat.productcategory_name
        from stg_product prod
        left join stg_productmodel pm on prod.fk_productmodelid = pm.pk_productmodelid
        left join stg_productsubcategory psubcat on prod.fk_productsubcategoryid = psubcat.pk_productsubcategoryid
        left join stg_productcategory pcat on psubcat.fk_productcategoryid = pcat.pk_productcategoryid
    )

select
    pk_productid as product_id
    ,product_name
    ,productnumber
    ,makeflag
    ,finishedgoodsflag
    ,color
    ,safetystocklevel
    ,reorderpoint
    ,standardcost
    ,listprice
    ,size
    ,sizeunitmeasurecode
    ,weightunitmeasurecode
    ,product_weight
    ,daystomanufacture
    ,productline
    ,product_class
    ,product_style
    ,sellstartdate
    ,sellenddate
    ,discontinueddate
    ,productmodel_name
    ,catalogdescription
    ,instructions
    ,productsubcategory_name
    ,productcategory_name

from joined