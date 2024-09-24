-- models/marts/dimensions/dim_address.sql

with
    address as (
        select
            pk_addressid,
            addressline1,
            city,
            fk_stateprovinceid,
            postalcode,
            spatiallocation,
            rowguid,
            modifieddate

        from {{ ref("stg_adventurework_erp__Address") }}
    ),

    stateprovince as (
        select
            pk_stateprovinceid,
            stateprovincecode,
            fk_countryregioncode,
            isonlystateprovinceflag,
            stateprovince_name,
            fk_territoryid,
            rowguid,
            modifieddate

        from {{ ref("stg_adventurework_erp__STATEPROVINCE") }}
    ),

    countryregion as (
        select
            pk_countryregioncode,
            name as country_name,
            modifieddate

        from {{ ref("stg_adventurework_erp__CountryRegion") }}
    ),

    joined as (
        select
            addr.pk_addressid,
            addr.addressline1,
            addr.city,
            addr.postalcode,
            addr.spatiallocation,
            stateprov.stateprovincecode,
            stateprov.stateprovince_name,
            stateprov.isonlystateprovinceflag,
            stateprov.fk_countryregioncode,
            country.country_name

        from address addr
        left join stateprovince stateprov on addr.fk_stateprovinceid = stateprov.pk_stateprovinceid
        left join countryregion country on stateprov.fk_countryregioncode = country.pk_countryregioncode
    )

select
    pk_addressid as address_id,
    addressline1,
    city,
    postalcode,
    spatiallocation,
    stateprovincecode,
    stateprovince_name,
    isonlystateprovinceflag,
    fk_countryregioncode as countryregion_code,
    country_name

from joined
