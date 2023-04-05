drop table if exists public.shipping_country_rates;

create table public.shipping_country_rates (
	id serial,
	shipping_country text not null,
	shipping_country_base_rate numeric(14,2) not null,
	CONSTRAINT shipping_country_rates_pkey PRIMARY KEY (id)
);

insert into public.shipping_country_rates (shipping_country, SHIPPING_COUNTRY_BASE_RATE)
select distinct 
	shipping_country, 
	SHIPPING_COUNTRY_BASE_RATE  
from shipping;
-----------------------------------------------------------------------
drop table if exists public.shipping_agreement;

create table public.shipping_agreement (
	agreement_id serial,
	agreement_number text not null,
	agreement_rate	numeric(14,2) not null,
	agreement_commission numeric(14,2) not null,
	CONSTRAINT shipping_agreement_pkey PRIMARY KEY (agreement_id)
);

insert into public.shipping_agreement
select distinct
	(regexp_split_to_array(vendor_agreement_description, ':+'))[1]::int,
	(regexp_split_to_array(vendor_agreement_description, ':+'))[2],
	(regexp_split_to_array(vendor_agreement_description, ':+'))[3]::numeric(14,2),
	(regexp_split_to_array(vendor_agreement_description, ':+'))[4]::numeric(14,2)
from SHIPPING; 	
-----------------------------------------------------------------------

drop table if exists public.shipping_transfer;

create table public.shipping_transfer (
	id serial,
	transfer_type text not null,
	transfer_model text not null,
	shipping_transfer_rate numeric(14,2) not null,
	CONSTRAINT shipping_transfer_pkey PRIMARY KEY (id)
);

create sequence shipping_transfer_seq start 1;

insert into public.shipping_transfer
select	NEXTVAL('shipping_transfer_seq'), 
		transfer_type,
		transfer_model,
		SHIPPING_TRANSFER_RATE
from (		
		select distinct
			(regexp_split_to_array(shipping_transfer_description , ':+'))[1] as transfer_type,
			(regexp_split_to_array(shipping_transfer_description , ':+'))[2] as transfer_model, 
			SHIPPING_TRANSFER_RATE 
		from SHIPPING 
	 ) X;

drop sequence shipping_transfer_seq;
----------------------------------------------------------

drop table if exists public.shipping_info;

create table public.shipping_info (
	shipping_id bigserial,
	shipping_country_rates_id int not null,
	shipping_agreement_id int not null,
	shipping_transfer_id int not null,
	shipping_plan_datetime timestamp,
	vendor_id int,
	payment_amount numeric(14,2),
	CONSTRAINT shipping_info_pkey PRIMARY KEY (shipping_id),
	constraint shipping_country_rates_fk foreign key(shipping_country_rates_id) references public.shipping_country_rates(id),
	constraint shipping_agreement_fk foreign key(shipping_agreement_id) references public.shipping_agreement(agreement_id),
	constraint shipping_transfer_fk foreign key(shipping_transfer_id) references public.shipping_transfer(id)
);

insert into public.shipping_info
select DISTINCT 
	SHIPPINGID,
	r.id,
	a.agreement_id,
	t.id,
	s.shipping_plan_datetime,
	s.VENDORID,
	s.payment_amount
	
from SHIPPING S
	join public.shipping_country_rates r 
		on s.shipping_country = r.shipping_country
		and s.shipping_country_base_rate = r.shipping_country_base_rate
	join public.shipping_agreement a 
		on (a.agreement_id::text||':'||
		    a.agreement_number||':'||
		    a.agreement_rate::text||':'||
		    a.agreement_commission) = s.vendor_agreement_description
	join public.shipping_transfer t 
		on (t.transfer_type||':'||t.transfer_model) = s.shipping_transfer_description 
		and t.SHIPPING_TRANSFER_RATE = s.SHIPPING_TRANSFER_RATE;   
		
-------------------------------------------------------------
	drop table if exists public.shipping_status; 
	
	create table public.shipping_status (
		id bigserial,
		shipping_id int,
		status text,
		state text,
		shipping_start_fact_datetime timestamp,
		shipping_end_fact_datetime timestamp,
		CONSTRAINT shipping_status_pkey PRIMARY KEY (id)	
	);
	
	create sequence shipping_status_seq start 1;
	
	insert into public.shipping_status
	select 
		NEXTVAL('shipping_status_seq'), 
		s.SHIPPINGID, 
		STATUS,
		state,
		mn,
		mx
	
	from SHIPPING S
		join (
			select	SHIPPINGID, 	
					min(state_datetime) as mn,
					max(state_datetime) as mx
 			from SHIPPING 
 			group by SHIPPINGID
		) X
		on X.SHIPPINGID = s.SHIPPINGID 
			and X.mx = s.state_datetime

drop sequence shipping_status_seq;			
------------------------------------------------------------------
drop view if exists shipping_datamart;

create view shipping_datamart
as
select 
	si.shipping_id,
	si.vendor_id,
	t.transfer_type,
	EXTRACT(DAY FROM st.shipping_end_fact_datetime - st.shipping_start_fact_datetime),
	case when st.shipping_end_fact_datetime > si.shipping_plan_datetime 
		 then 1 else 0 
	end as is_delay,
	case when st.status = 'finished'
		 then 1 else 0
	end as is_shipping_finish,
	case when st.shipping_end_fact_datetime > si.shipping_plan_datetime 
	     then EXTRACT(DAY FROM st.shipping_end_fact_datetime - si.shipping_plan_datetime)
	     else 0
	end as delay_day_at_shipping,
	si.payment_amount,
	si.payment_amount * (sr.shipping_country_base_rate + sa.agreement_rate + t.SHIPPING_TRANSFER_RATE) as vat,
	si.payment_amount * sa.agreement_commission as profit  
FROM public.shipping_info si
	join public.shipping_transfer t
		on t.id = si.shipping_transfer_id
	join public.shipping_status st
		on st.shipping_id = si.shipping_id
	join public.shipping_country_rates sr
		on sr.id = si.shipping_country_rates_id
	join public.shipping_agreement sa 
		on sa.agreement_id = si.shipping_agreement_id;
