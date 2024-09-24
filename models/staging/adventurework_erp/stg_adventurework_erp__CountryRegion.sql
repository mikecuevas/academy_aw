with 

source as (
      
      select * from {{ source('adventurework_erp', 'CountryRegion') }}

),
countryregion as (
    select
        
        cast(COUNTRYREGIONCODE as varchar) as PK_COUNTRYREGIONCODE
        ,cast(NAME as varchar) as NAME
        ,cast(MODIFIEDDATE as date) as MODIFIEDDATE

    from source
)
select * from countryregion
  