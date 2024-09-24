with

source as (

    select * from {{ source('adventurework_erp', 'Employee') }}

),

employee as (

    select
        cast(businessentityid as int) as pk_businessentityid,
        cast(nationalidnumber as varchar) as nationalidnumber,
        cast(loginid as varchar) as loginid,
        cast(jobtitle as varchar) as jobtitle,
        cast(birthdate as date) as birthdate,
        cast(maritalstatus as varchar) as maritalstatus,
        cast(gender as varchar) as gender,
        cast(hiredate as date) as hiredate,
        cast(salariedflag as boolean) as salariedflag,
        cast(vacationhours as int) as vacationhours,
        cast(sickleavehours as int) as sickleavehours,
        cast(currentflag as boolean) as currentflag,
        cast(rowguid as varchar) as rowguid,
        cast(modifieddate as date) as modifieddate

    from source

)

select * from employee
