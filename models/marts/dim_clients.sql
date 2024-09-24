with

    customer as (
        select
            pk_customerid,
            fk_personid,
            fk_storeid,
            fk_territoryid,
            rowguid as customer_rowguid,
            modifieddate as customer_modifieddate

        from {{ ref("stg_adventurework_erp__Customer") }}
    ),

    person as (
        select
            pk_businessentityid,
            persontype,
            namestyle,
            title,
            firstname,
            middlename,
            lastname,
            suffix,
            emailpromotion,
            additionalcontactinfo,
            demographics,
            rowguid as person_rowguid,
            modifieddate as person_modifieddate

        from {{ ref("stg_adventurework_erp__Person") }}
    ),

    businessentityaddress as (
        select
            fk_businessentityid,
            fk_addressid,
            fk_addresstypeid,
            rowguid,
            modifieddate

        from {{ ref("stg_adventurework_erp__BusinessEntityAddress") }}
    ),

    joined as (
        select
            cust.pk_customerid,
            cust.fk_personid,
            cust.fk_storeid,
            cust.fk_territoryid,
            cust.customer_rowguid,
            cust.customer_modifieddate,

            pers.persontype,
            pers.namestyle,
            pers.title,
            pers.firstname,
            pers.middlename,
            pers.lastname,
            pers.suffix,
            pers.emailpromotion,
            pers.additionalcontactinfo,
            pers.demographics,
            pers.person_rowguid,
            pers.person_modifieddate,

            bea.fk_addressid,
            addr.addressline1,
            addr.city,
            addr.stateprovincecode,
            addr.stateprovince_name,
            addr.countryregion_code,
            addr.country_name

        from customer cust
        left join person pers on cust.fk_personid = pers.pk_businessentityid
        left join businessentityaddress bea on pers.pk_businessentityid = bea.fk_businessentityid
        left join {{ ref('dim_address') }} addr on bea.fk_addressid = addr.address_id
    )

select
    pk_customerid as customer_id,
    fk_personid as person_id,
    fk_storeid as store_id,
    fk_territoryid as territory_id,

    firstname,
    middlename,
    lastname,
    emailpromotion,

    addressline1,
    city,
    stateprovincecode,
    stateprovince_name,
    countryregion_code,
    country_name

from joined
