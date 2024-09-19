with 

source as (

    select * from {{ source('adventurework_erp', 'Product') }}

),

renamed as (

    select
        productid,
        name,
        productnumber,
        makeflag,
        finishedgoodsflag,
        color,
        safetystocklevel,
        reorderpoint,
        standardcost,
        listprice,
        size,
        sizeunitmeasurecode,
        weightunitmeasurecode,
        weight,
        daystomanufacture,
        productline,
        class,
        style,
        productsubcategoryid,
        productmodelid,
        sellstartdate,
        sellenddate,
        discontinueddate,
        rowguid,
        modifieddate

    from source

)

select * from renamed
