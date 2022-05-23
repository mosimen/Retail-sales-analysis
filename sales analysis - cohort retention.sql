--DATA CLEANING

--Task A,
--we see records without 
--1.customer id
--2.quantity than zero
--3.price less than zero

--Task B,
--we will also check for duplicates in the dataset

drop table if exists #retail_data
;with retaildata as
(select *
from retail_data
where CustomerID !=0 and Quantity>0 and UnitPrice>0),

---checking for duplicates
dup_check as
(
select *,
	ROW_NUMBER() over(partition by invoiceno, stockcode, quantity order by invoicedate) as dupflag
from retaildata)

select *
into #retail_data
from dup_check
WHERE dupflag=1

select * from #retail_data

---TIME BASED COHORT ANALYSIS
--This is carried out to understand the behaviour of group of customers,
--we are looking out for trends, patterns

--metrics of  cohort analysis
--1. unique identifier i.e customerid
--2. initial start date i.e first date a customer made a purchase
--3. revenue

drop table if exists #cohort
select CustomerID,
	min(InvoiceDate) as firstinvoicedate,
	DATEFROMPARTS(YEAR(min(InvoiceDate)),MONTH(min(InvoiceDate)),1) as cohortdate
into #cohort
from #retail_data
group by CustomerID

--Next we create a cohort index
--This is a representation of the number of months that has passed since the customer's
--first purchase

drop table if exists #cohortretention
select ct2.*,
	cohortindex=year_diff * 12 + month_diff + 1
into #cohortretention
from (
	select ct.*,
		year_diff=invoiceyear-cohortyear,
		month_diff=invoicemonth-cohortmonth
	from(
		select rd.*,
			cd.cohortdate,
			YEAR(rd.invoicedate) invoiceyear,
			month(rd.InvoiceDate) invoicemonth,
			year(cd.cohortdate) cohortyear,
			month(cd.cohortdate) cohortmonth
		from #retail_data as rd
		left join #cohort as cd
		on rd.CustomerID=cd.CustomerID) as ct
) as ct2

select * from #cohortretention

select distinct CustomerID,
	cohortdate,
	cohortindex
from #cohortretention
order by 1,3

--cohortindex 1 means the customer made their next purchase in the same month they made their first 
--purchase

---Pivot data to see the cohort table
drop table if exists #cohortpivot
select *
into #cohortpivot
from (
	select distinct CustomerID,
		cohortdate,
		cohortindex
	from #cohortretention) as cohtab
pivot(
	count(customerid)
	for cohortindex in 
		([1],[2],[3],[4],[5],[6],[7],[8],[9],
		[10],[11],[12],[13])
) as pvt

select * from #cohortpivot
order by 1 



