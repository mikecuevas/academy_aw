with 

source as (

    select * from {{ source('adventurework_erp', 'STATEPROVINCE') }}

),

stateprovince as (

    select
        cast(stateprovinceid as int) as pk_stateprovinceid
        ,cast(territoryid as int) as fk_territoryid
        ,cast(countryregioncode as varchar) as fk_countryregioncode
        ,cast(stateprovincecode as varchar) as stateprovincecode
        ,cast(isonlystateprovinceflag as boolean) as isonlystateprovinceflag
        ,cast(name as varchar) as stateprovince_name
        ,cast(rowguid as varchar) as rowguid
        ,cast(modifieddate as timestamp) as modifieddate

    from source

)

select * from stateprovince
