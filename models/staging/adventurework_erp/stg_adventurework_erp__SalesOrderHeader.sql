with 

source as (

    select * from {{ source('adventurework_erp', 'SalesOrderHeader') }}

),

salesorderheader as (

    select
        cast(salesorderid as int) as pk_salesorderid
        ,cast(customerid as int) as fk_customerid
        ,cast(salespersonid as int) as fk_salespersonid
        ,cast(territoryid as int) as fk_territoryid
        ,cast(billtoaddressid as int) as fk_billtoaddressid
        ,cast(shiptoaddressid as int) as fk_shiptoaddressid
        ,cast(shipmethodid as int) as fk_shipmethodid
        ,cast(creditcardid as int) as fk_creditcardid
        ,cast(currencyrateid as int) as fk_currencyrateid
        ,cast(revisionnumber as int) as revisionnumber
        ,cast(orderdate as date) as orderdate
        ,cast(duedate as date) as duedate
        ,cast(shipdate as date) as shipdate
        ,cast(status as int) as status
        ,cast(onlineorderflag as int) as onlineorderflag
        ,cast(accountnumber as varchar) as accountnumber
        ,cast(creditcardapprovalcode as varchar) as creditcardapprovalcode
        ,cast(subtotal as float) as subtotal
        ,cast(taxamt as float) as taxamt
        ,cast(freight as float) as freight
        ,cast(totaldue as float) as totaldue
        ,cast(rowguid as varchar) as rowguid
        ,cast(modifieddate as date) as modifieddate

    from source

)

select * from salesorderheader
