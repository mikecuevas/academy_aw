with 

source as (

    select * from {{ source('adventurework_erp', 'SalesReason') }}

),

renamed as (

    select
        salesreasonid,
        name,
        reasontype,
        modifieddate

    from source

)

select * from renamed
