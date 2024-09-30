with 

source as (
      
      select * from {{ source('adventurework_erp', 'CountryRegion') }}

),
countryregion as (
    select
        cast(countryregioncode as varchar) as pk_countryregioncode
        , cast(name as varchar) as country_name
    from source
)
select * from countryregion
  