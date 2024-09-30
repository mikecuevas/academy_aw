with
    product as (
        select
            pk_productid
          , name
          , productnumber
          , makeflag
          , finishedgoodsflag
          , color
          , standardcost
          , listprice
          , size
          , sizeunitmeasurecode
          , weight
          , weightunitmeasurecode
          , daystomanufacture
          , fk_productsubcategoryid
          , fk_productmodelid
        from {{ ref('stg_adventurework_erp__Product') }}
    ),

    productsubcategory as (
        select
            pk_productsubcategoryid
          , productsub_name
          , fk_productcategoryid
        from {{ ref('stg_adventurework_erp__ProductSubCategory') }}
    ),

    productcategory as (
        select
            pk_productcategoryid
          , productcategory_name
        from {{ ref('stg_adventurework_erp__ProductCategory') }}
    ),

    productmodel as (
        select
            pk_productmodelid
          , productmodel_name
        from {{ ref('stg_adventurework_erp__ProductModel') }}
    ),

    product_data as (
        select
            prod.pk_productid
          , prod.name
          , prod.productnumber
          , prod.makeflag
          , prod.finishedgoodsflag
          , prod.color
          , prod.standardcost
          , prod.listprice
          , prod.size
          , prod.sizeunitmeasurecode
          , prod.weight
          , prod.weightunitmeasurecode
          , prod.daystomanufacture
          , subcat.productsub_name
          , cat.productcategory_name
          , model.productmodel_name
        from product prod
        left join productsubcategory subcat on prod.fk_productsubcategoryid = subcat.pk_productsubcategoryid
        left join productcategory cat on subcat.fk_productcategoryid = cat.pk_productcategoryid
        left join productmodel model on prod.fk_productmodelid = model.pk_productmodelid
    ),

    product_with_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(['pk_productid']) }} as product_sk
          , pk_productid
          , name
          , productnumber
          , makeflag
          , finishedgoodsflag
          , color
          , standardcost
          , listprice
          , size
          , sizeunitmeasurecode
          , weight
          , weightunitmeasurecode
          , daystomanufacture
          , productsub_name
          , productcategory_name
          , productmodel_name
        from product_data
    )

select * from product_with_sk
