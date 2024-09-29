with

    stg_customer as (
        select
            pk_customerid
            ,fk_personid
            ,fk_storeid,
            ,fk_territoryid

        from {{ ref("stg_adventurework_erp__Customer") }}
    ),

    stg_person as (
        select
            pk_businessentityid
            ,persontype
            ,namestyle
            ,title
            ,firstname
            ,middlename
            ,lastname
            ,suffix
            ,emailpromotion
            ,additionalcontactinfo
            ,demographics

        from {{ ref("stg_adventurework_erp__Person") }}
    ),

    stg_businessentityaddress as (
        select
            fk_businessentityid
            ,fk_addressid
            ,fk_addresstypeid

        from {{ ref("stg_adventurework_erp__BusinessEntityAddress") }}
    ),

    joined as (
        select
            cust.pk_customerid
            ,cust.fk_personid
            ,cust.fk_storeid
            ,cust.fk_territoryid
            ,pers.persontype
            ,pers.namestyle
            ,pers.title
            ,pers.firstname
            ,pers.middlename
            ,pers.lastname
            ,pers.suffix
            ,pers.emailpromotion
            ,pers.additionalcontactinfo
            ,pers.demographics
            ,bea.fk_addressid
            ,addr.addressline1
            ,addr.city
            ,addr.stateprovincecode
            ,addr.stateprovince_name
            ,addr.countryregion_code
            ,addr.country_name

        from stg_customer cust
        left join stg_person pers on cust.fk_personid = pers.pk_businessentityid
        left join stg_businessentityaddress bea on pers.pk_businessentityid = bea.fk_businessentityid
        left join {{ ref('dim_address') }} addr on bea.fk_addressid = addr.address_id
    )

select
    pk_customerid
    ,fk_personid
    ,fk_storeid
    ,fk_territoryid
    ,firstname
    ,middlename
    ,lastname
    ,emailpromotion
    ,addressline1
    ,city
    ,stateprovincecode
    ,stateprovince_name
    ,countryregion_code
    ,country_name

from joined
