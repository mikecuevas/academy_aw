with 

source as (

    select * from {{ source('adventurework_erp', 'ProductModel') }}

),

renamed as (

    select
        productmodelid,
        name,
        catalogdescription,
        instructions,
        rowguid,
        modifieddate

    from source

)

select * from renamed
