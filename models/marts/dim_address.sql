with
    address as (
        select
            pk_addressid
          , addressline1
          , city
          , postalcode
          , spatiallocation
          , fk_stateprovinceid
        from {{ ref('stg_adventurework_erp__Address') }}
    ),

    stateprovince as (
        select
            pk_stateprovinceid
          , stateprovincecode
          , stateprovince_name
          , isonlystateprovinceflag
          , fk_countryregioncode
        from {{ ref('stg_adventurework_erp__STATEPROVINCE') }}
    ),

    countryregion as (
        select
            pk_countryregioncode
          , country_name
        from {{ ref('stg_adventurework_erp__CountryRegion') }}
    ),

    joined as (
        select
          {{ dbt_utils.generate_surrogate_key(['pk_addressid']) }} as address_sk
          , addr.pk_addressid
          , addr.addressline1
          , addr.city
          , addr.postalcode
          , addr.spatiallocation
          , stateprov.stateprovincecode
          , stateprov.stateprovince_name
          , stateprov.isonlystateprovinceflag
          , stateprov.fk_countryregioncode
          , country.country_name
        from address addr
        left join stateprovince stateprov on addr.fk_stateprovinceid = stateprov.pk_stateprovinceid
        left join countryregion country on stateprov.fk_countryregioncode = country.pk_countryregioncode
    )

select
  address_sk
  , pk_addressid
  , fk_countryregioncode
  , addressline1
  , city
  , postalcode
  , spatiallocation
  , stateprovincecode
  , stateprovince_name
  , isonlystateprovinceflag
  , country_name
from joined
