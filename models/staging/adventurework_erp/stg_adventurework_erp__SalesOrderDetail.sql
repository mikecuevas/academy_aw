with 

source as (

    select * from {{ source('adventurework_erp', 'SalesOrderDetail') }}

),

salesorderdetail as (

    select
        cast(salesorderid as int) as pk_salesorderid
        ,cast(salesorderdetailid as int) as pk_salesorderdetailid
        ,cast(specialofferid as int) as fk_specialofferid
        ,cast(productid as int) as fk_productid
        ,cast(carriertrackingnumber as varchar) as carriertrackingnumber
        ,cast(orderqty as int) as orderqty
        ,cast(unitprice as float) as unitprice
        ,cast(unitpricediscount as float) as unitpricediscount
        ,cast(rowguid as varchar) as rowguid
        ,cast(modifieddate as date) as modifieddate

    from source

)

select * from salesorderdetail
