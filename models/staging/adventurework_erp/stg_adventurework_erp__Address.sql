with 

source as (

    select * from {{ source('adventurework_erp', 'Address') }}

),

renamed as (

    select
        addressid,
        addressline1,
        city,
        stateprovinceid,
        postalcode,
        spatiallocation,
        rowguid,
        modifieddate

    from source

)

select * from renamed
