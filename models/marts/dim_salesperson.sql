with
    salesperson as (
        select
            pk_businessentityid
          , fk_territoryid
          , salesquota
          , bonus
          , commissionpct
          , salesytd
          , saleslastyear
        from {{ ref('stg_adventurework_erp__SalesPerson') }}
    ),

    employee as (
        select
            pk_businessentityid
          , nationalidnumber
          , loginid
          , jobtitle
          , birthdate
          , maritalstatus
          , gender
          , hiredate
        from {{ ref('stg_adventurework_erp__Employee') }}
    ),

    person as (
        select
            pk_businessentityid
          , firstname
          , middlename
          , lastname
        from {{ ref('stg_adventurework_erp__Person') }}
    ),

    businessentityaddress as (
        select
            fk_businessentityid
          , fk_addressid
        from {{ ref('stg_adventurework_erp__BusinessEntityAddress') }}
    ),

    salesperson_data as (
        select
            sp.pk_businessentityid
          , sp.fk_territoryid
          , sp.salesquota
          , sp.bonus
          , sp.commissionpct
          , sp.salesytd
          , sp.saleslastyear
          , emp.jobtitle
          , emp.birthdate
          , emp.maritalstatus
          , emp.gender
          , emp.hiredate
          , pers.firstname
          , pers.middlename
          , pers.lastname
          , bea.fk_addressid
        from salesperson sp
        left join employee emp on sp.pk_businessentityid = emp.pk_businessentityid
        left join person pers on emp.pk_businessentityid = pers.pk_businessentityid
        left join businessentityaddress bea on pers.pk_businessentityid = bea.fk_businessentityid
    ),

    salesperson_with_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(['pk_businessentityid']) }} as salesperson_sk
          , pk_businessentityid
          , fk_territoryid
          , salesquota
          , bonus
          , commissionpct
          , salesytd
          , saleslastyear
          , jobtitle
          , birthdate
          , maritalstatus
          , gender
          , hiredate
          , firstname
          , middlename
          , lastname
          , fk_addressid
        from salesperson_data
    )

select * from salesperson_with_sk
