with 

source as (

    select * from {{ source('adventurework_erp', 'ProductCategory') }}

),

renamed as (

    select
        productcategoryid,
        name,
        rowguid,
        modifieddate

    from source

)

select * from renamed
