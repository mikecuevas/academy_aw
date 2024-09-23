with 

source as (

    select * from {{ source('adventurework_erp', 'STATEPROVINCE') }}

),

stateprovince as (

    select
        stateprovinceid,
        stateprovincecode,
        countryregioncode,
        isonlystateprovinceflag,
        name,
        territoryid,
        rowguid,
        modifieddate

    from source

)

select * from stateprovince
