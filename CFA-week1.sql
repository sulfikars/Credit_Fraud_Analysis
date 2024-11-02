create database data_project;
use data_project;


-- Create Customers table
CREATE TABLE Customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    date_of_birth DATE,
    email VARCHAR(100),
    phone_number VARCHAR(100),
    address VARCHAR(255),
    city VARCHAR(50),
    state VARCHAR(50),
    postal_code VARCHAR(10),
    country VARCHAR(50),
    annual_income DECIMAL(20, 2),
    employment_status VARCHAR(50),
    account_open_date DATE,
    credit_score INT
);

-- corrected the date format in excel and imported data to the excel table
select * from customers;

-- Create Transactions table
CREATE TABLE Transactions (
    transaction_id INT PRIMARY KEY,
    customer_id INT,
    transaction_date DATETIME,
    transaction_amount DECIMAL(15, 2),
    merchant_name VARCHAR(100),
    merchant_category VARCHAR(50),
    transaction_city VARCHAR(50),
    transaction_state VARCHAR(50),
    transaction_country VARCHAR(50),
    transaction_status VARCHAR(20)
);

-- corrected date and time values in excel and imported
select  * from transactions;

-- Create Accounts table
CREATE TABLE Accounts (
    account_id INT PRIMARY KEY,
    customer_id INT,
    account_type VARCHAR(50),
    credit_limit DECIMAL(15, 2),
    balance DECIMAL(15, 2),
    account_status VARCHAR(20),
    delinquent BOOLEAN
);

-- converted delinquent boolean values to 0 & 1 in excel and imported
select * from accounts;

-- Create Credit_History table
CREATE TABLE Credit_History (
    history_id INT PRIMARY KEY,
    customer_id INT,
    account_id INT,
    payment_date DATE,
    due_amount DECIMAL(15, 2),
    payment_amount DECIMAL(15, 2),
    missed_payment BOOLEAN,
    days_late INT
);

-- converted payment_date and missed_payment column to correct formats in excel and imported
select * from credit_history;

-- Create Fraud_Records table
CREATE TABLE Fraud_Records (
    fraud_id INT PRIMARY KEY,
    transaction_id INT,
    fraud_detected_date DATETIME,
    fraud_type VARCHAR(50),
    investigation_status VARCHAR(50),
    fraud_resolution VARCHAR(50)
);

-- converted date and time values in excel and imported
select * from fraud_records;

/* 
1. Extract customer and transaction data based on defined features (e.g., monthly transaction
totals, outlier detection on spending, etc.)
*/

-- monthly transaction totals
select 
    c.customer_id,
    c.first_name,
    c.last_name,
    year(t.transaction_date) as transaction_year,
    monthname(t.transaction_date) as transaction_month,
    sum(t.transaction_amount) as total_transaction
from 
    customers c
join 
    transactions t on c.customer_id = t.customer_id
group by 
    c.customer_id, transaction_year, transaction_month
order by 
    transaction_year, transaction_month, c.customer_id;
    
-- outlier detection on spending
with TransactionStats as(
	select customer_id, 
		avg(transaction_amount) as avg_spending,
		stddev(transaction_amount) as stddev_spending
    from 
		Transactions
    group by 
		customer_id
)
select t.transaction_id,
	t.customer_id,
    t.transaction_date,
    t.transaction_amount,
    ts.avg_spending,
    ts.stddev_spending,
    ts.avg_spending + (1.5 * ts.stddev_spending) as std_p_distance,
    ts.avg_spending - (1.5 * ts.stddev_spending) as std_n_distance
from 
	Transactions as t
join
	TransactionStats as ts
on
	t.customer_id = ts.customer_id
where
	t.transaction_amount > (ts.avg_spending + (1.5 * ts.stddev_spending))
    or t.transaction_amount < (ts.avg_spending - (1.5 * ts.stddev_spending));

-- 2. Perform joins between customers, transactions, accounts, and credit_history to gather
-- complete data per customer.
select
	c.customer_id,c.first_name,c.last_name,c.date_of_birth,c.email,c.phone_number,c.address,
    c.city,c.state,c.postal_code,c.country,c.annual_income,c.employment_status,c.account_open_date,c.credit_score,
    a.account_id,a.account_type,a.credit_limit,a.balance,a.account_status,a.delinquent,
	t.transaction_id,t.transaction_date,t.transaction_amount,t.merchant_name,t.merchant_category,
    t.transaction_city,t.transaction_state,t.transaction_country,t.transaction_status,
    ch.history_id,ch.payment_date,ch.due_amount,ch.payment_amount,ch.missed_payment,ch.days_late
from
	customers as c
left join
	accounts as a on c.customer_id=a.customer_id
left join
	transactions as t on c.customer_id=t.customer_id
left join 
	credit_history as ch on c.customer_id=ch.customer_id and a.account_id=ch.account_id
order by
	c.customer_id, t.transaction_date, ch.payment_date;

/* Generate new features such as:
o Average transaction amounts by time periods (daily/weekly).
o Transaction location consistency (using geographical data).
o Delinquency rates from credit_history.
*/

-- Average transaction amounts by time periods - Daily
select 
    date(transaction_date) as transaction_day,
    avg(transaction_amount) as average_daily_transaction
from 
    transactions
group by 
    transaction_day
order by 
    transaction_day;


-- Average transaction amounts by time periods - Weekly
select
	year(transaction_date) as transaction_year,
    week(transaction_date) as transaction_week,
    avg(transaction_amount) as average_weekly_transaction
from
	transactions
group by
	transaction_year, transaction_week
order by
	transaction_year, transaction_week;
    
-- Transaction location consistency
select 
    c.customer_id,
    count(distinct concat(t.transaction_city, ', ', t.transaction_state)) as unique_locations,
    count(t.transaction_id) as total_transactions,
    case 
        when count(distinct concat(t.transaction_city, ', ', t.transaction_state)) / count(t.transaction_id) < 0.5 then 'low'
        when count(distinct concat(t.transaction_city, ', ', t.transaction_state)) / count(t.transaction_id) < 1 then 'medium'
        else 'high'
    end as location_consistency
from 
    customers as c
left join 
    transactions as t on c.customer_id = t.customer_id
group by 
    c.customer_id
order by 
    c.customer_id;


-- Delinquency Rates from Credit History

select 
    c.customer_id,
    concat(c.first_name, ' ', c.last_name) as cust_name,
    count(case when a.delinquent = 1 then 1 end) as delinquent_accounts,
    count(a.account_id) as total_accounts,
    case 
        when count(a.account_id) = 0 then 0
        else count(case when a.delinquent = 1 then 1 end) / count(a.account_id)
    end as delinquency_rate
from 
    customers c
left join 
    accounts a on c.customer_id = a.customer_id
group by 
    c.customer_id
order by 
    c.customer_id;