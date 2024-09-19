with 

source as (

    select * from {{ source('adventurework_erp', 'SalesOrderDetail') }}

),

renamed as (

    select
        salesorderid,
        salesorderdetailid,
        carriertrackingnumber,
        orderqty,
        productid,
        specialofferid,
        unitprice,
        unitpricediscount,
        rowguid,
        modifieddate

    from source

)

select * from renamed
