with
    stg_salesperson as (
        select
            pk_businessentityid
            ,fk_territoryid
            ,salesquota
            ,bonus
            ,commissionpct
            ,salesytd
            ,saleslastyear

        from {{ ref("stg_adventurework_erp__SalesPerson") }}
    )
    , stg_employee as (
        select
            pk_businessentityid
            ,nationalidnumber
            ,loginid
            ,jobtitle
            ,birthdate
            ,maritalstatus
            ,gender
            ,hiredate
            ,salariedflag
            ,vacationhours
            ,sickleavehours
            ,currentflag

        from {{ ref("stg_adventurework_erp__Employee") }}
    )
    , stg_person as (
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
    )
    , stg_businessentityaddress as (
        select
            fk_businessentityid,
            fk_addressid,
            fk_addresstypeid,
            rowguid,
            modifieddate

        from {{ ref("stg_adventurework_erp__BusinessEntityAddress") }}
    )
    , joined as (
        select
            sp.pk_businessentityid
            ,sp.fk_territoryid
            ,sp.salesquota
            ,sp.bonus
            ,sp.commissionpct
            ,sp.salesytd
            ,sp.saleslastyear
            ,emp.nationalidnumber
            ,emp.loginid
            ,emp.jobtitle
            ,emp.birthdate
            ,emp.maritalstatus
            ,emp.gender
            ,emp.hiredate
            ,pers.firstname
            ,pers.middlename
            ,pers.lastname
            ,bea.fk_addressid
            ,addr.addressline1
            ,addr.city
            ,addr.stateprovincecode
            ,addr.stateprovince_name
            ,addr.countryregion_code
            ,addr.country_name

        from stg_salesperson sp
        left join stg_employee emp on sp.pk_businessentityid = emp.pk_businessentityid
        left join stg_person pers on emp.pk_businessentityid = pers.pk_businessentityid
        left join stg_businessentityaddress bea on pers.pk_businessentityid = bea.fk_businessentityid
        left join {{ ref('dim_address') }} addr on bea.fk_addressid = addr.address_id
    )

select
    pk_businessentityid
    ,fk_territoryid
    ,salesquota
    ,bonus
    ,commissionpct
    ,salesytd
    ,saleslastyear
    ,jobtitle
    ,birthdate
    ,maritalstatus
    ,gender
    ,hiredate
    ,firstname
    ,middlename
    ,lastname
    ,addressline1
    ,city
    ,stateprovincecode
    ,stateprovince_name
    ,countryregion_code
    ,country_name
from joined
