-- models/marts/dimensions/dim_salesperson.sql

with
    salesperson as (
        select
            pk_businessentityid,
            fk_territoryid,
            salesquota,
            bonus,
            commissionpct,
            salesytd,
            saleslastyear,
            rowguid as salesperson_rowguid,
            modifieddate as salesperson_modifieddate

        from {{ ref("stg_adventurework_erp__SalesPerson") }}
    ),

    employee as (
        select
            pk_businessentityid,
            nationalidnumber,
            loginid,
            jobtitle,
            birthdate,
            maritalstatus,
            gender,
            hiredate,
            salariedflag,
            vacationhours,
            sickleavehours,
            currentflag,
            rowguid as employee_rowguid,
            modifieddate as employee_modifieddate

        from {{ ref("stg_adventurework_erp__Employee") }}
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
            sp.pk_businessentityid as salesperson_id,
            sp.fk_territoryid as territory_id,
            sp.salesquota,
            sp.bonus,
            sp.commissionpct,
            sp.salesytd,
            sp.saleslastyear,

            emp.nationalidnumber,
            emp.loginid,
            emp.jobtitle,
            emp.birthdate,
            emp.maritalstatus,
            emp.gender,
            emp.hiredate,

            pers.firstname,
            pers.middlename,
            pers.lastname,

            bea.fk_addressid,
            addr.addressline1,
            addr.city,
            addr.stateprovincecode,
            addr.stateprovince_name,
            addr.countryregion_code,
            addr.country_name

        from salesperson sp
        left join employee emp on sp.pk_businessentityid = emp.pk_businessentityid
        left join person pers on emp.pk_businessentityid = pers.pk_businessentityid
        left join businessentityaddress bea on pers.pk_businessentityid = bea.fk_businessentityid
        left join {{ ref('dim_address') }} addr on bea.fk_addressid = addr.address_id
    )

select
    salesperson_id,
    territory_id,
    salesquota,
    bonus,
    commissionpct,
    salesytd,
    saleslastyear,

    jobtitle,
    birthdate,
    maritalstatus,
    gender,
    hiredate,

    firstname,
    middlename,
    lastname,

    addressline1,
    city,
    stateprovincecode,
    stateprovince_name,
    countryregion_code,
    country_name

from joined
