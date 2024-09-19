with 

source as (

    select * from {{ source('adventurework_erp', 'Customer') }}

),

renamed as (

    select
        customerid,
        personid,
        storeid,
        territoryid,
        rowguid,
        modifieddate

    from source

)

select * from renamed
