with
    stg_address as (
        select
            pk_addressid
            ,addressline1
            ,city
            ,fk_stateprovinceid
            ,postalcode
            ,spatiallocation

        from {{ ref("stg_adventurework_erp__Address") }}
    ),

    stg_stateprovince as (
        select
            pk_stateprovinceid
            ,fk_countryregioncode
            ,fk_territoryid
            ,stateprovincecode
            ,isonlystateprovinceflag
            ,stateprovince_name
            
        from {{ ref("stg_adventurework_erp__StateProvince") }}
    ),

    stg_countryregion as (
        select
            pk_countryregioncode
            ,name as country_name
            ,modifieddate

        from {{ ref("stg_adventurework_erp__CountryRegion") }}
    ),

    joined as (
        select
             {{ dbt_utils.generate_surrogate_key(['pk_addressid']) }} as address_sk -- Geração da surrogate key
            ,addr.addressline1
            ,addr.city
            ,addr.postalcode
            ,addr.spatiallocation
            ,stateprov.stateprovincecode
            ,stateprov.stateprovince_name
            ,stateprov.isonlystateprovinceflag
            ,stateprov.fk_countryregioncode
            ,country.country_name

        from address addr
        left join stateprovince stateprov 
            on addr.fk_stateprovinceid = stateprov.pk_stateprovinceid
        left join countryregion country 
            on stateprov.fk_countryregioncode = country.pk_countryregioncode
    )

select * from joined
