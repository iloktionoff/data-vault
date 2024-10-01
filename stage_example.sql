-- DROP FUNCTION siebel.make_le_account(bool);

CREATE OR REPLACE FUNCTION siebel.make_le_account(p_log boolean DEFAULT true)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
  declare
    p_out_mls_code varchar     := 'landing';
    p_out_module   varchar     := 'make_le_account';
    p_out_schema   varchar     := 'siebel';
    p_out_table    varchar	   := 'le_account';
    p_out_batch_id varchar     := ctrl.get_batch_id();
    p_out_start_ts timestamp;
    p_out_end_ts   timestamp;
    p_out_rn       int4;
    p_out_note     varchar;
    v_landing_ts   timestamp;
    v_max_tt_queue_ts   timestamp;
  begin
	  
    -------------------------- start logging -------------------------
      p_out_table    := 'le_account';
      p_out_note     := 'Populate';
      p_out_rn       := -1;
      p_out_start_ts := timezone('utc-3'::text, clock_timestamp() );
      v_landing_ts   := timezone('utc-3'::text, now() );
    ------------------------------------------------------------------
 	drop table if exists tt_queue
 	;
    create temporary table tt_queue on commit preserve rows as
      select sys_load_ts, acc_row_id, 
      		"acc_last_upd", "acc_atc_kpp", "acc_company_status", "acc_alias_name", "acc_location", "acc_email", "acc_main_fax_number", "acc_main_phone_number", "acc_name", "acc_master_id", "acc_partytypecode", "acc_partyuid", "acc_pershing_id", "acc_ou_type_cd", "acc_active_flag", "acc_bank", "acc_birthday", "acc_branch", "acc_broker_inst", "acc_x_calypso", "acc_certificate", "acc_certification_date", "acc_confirmation_level", "acc_confirmation_method", "acc_counterparty_inst", "acc_country_domicile", "acc_country_of_risk", "acc_create_sec_cus_det", "acc_date_first_contact", "acc_default_commission", "acc_x_diasoft", "acc_division_code", "acc_expiration_date", "acc_fsa_ref", "acc_family_status", "acc_x_fidessa", "acc_former_names", "acc_fund_id", "acc_inn", "acc_industry", "acc_institution", "acc_issued_by", "acc_issue_date", "acc_next_review_code", "acc_micex_client", "acc_micex_code", "acc_midas_number", "acc_market_info_5", "acc_par_ou_id", "acc_x_midas", "acc_name_in_english", "acc_oasys_code", "acc_parent", "acc_parent_name", "acc_passport_number", "acc_passport_seria", "acc_passport_type", "acc_price_accuracy", "acc_rts_by_in", "acc_rts_code", "acc_ready_for_export", "acc_registered_by", "acc_residence", "acc_swift_financial", "acc_swift_identifier", "acc_sex", "acc_tax", "acc_vtbc_adopted", "acc_vtbe_adopted", "acc_vtbcadditionaladoptions", "acc_ctm_code", "acc_emir_cntp_code", "acc_vtbc_mic_code", "acc_vtbi_adopted", "acc_swiss_midas", "acc_vtbt_adopted", "acc_hasaccounts", "acc_bookingpriceroundscale", "acc_bookingsettlepriceroundscale", "acc_fixclientsidentifiers", "acc_isgiveupbrokerflag", "acc_segment", "acc_more_types_jsonb", "acc_adoptions_jsonb", "acc_cut_address_jsonb"
      		, acc_nifi_filename, acc_kafka_topic, acc_kafka_partition, acc_kafka_offset, acc_kafka_timestamp
        from siebel.le_account_tt as tt
        join ctrl.get_offset_landing(p_out_schema, p_out_table) as ot 
    		on tt.sys_load_ts  > ot.p_out_ts_id
        where (1=1)
        	--and to_timestamp(acc_kafka_timestamp/1000)::timestamp > (select coalesce(max(effective_from),'1900-01-01') from siebel.le_account) 
        	and tt.sys_load_ts < v_landing_ts
	;

	v_max_tt_queue_ts := (select coalesce(max(sys_load_ts),'1900-01-01') from tt_queue)
	;
	--RAISE NOTICE 'v_max_tt_queue_ts is: %',v_max_tt_queue_ts;

	drop table if exists hashdiff_table
 	;
    create temporary table hashdiff_table on commit preserve rows as
      select effective_from, acc_row_id, hashdiff 
		from ctrl.get_hashdiff_table('tt_queue', 
			'{"to_timestamp(acc_kafka_timestamp/1000)::timestamp as effective_from", "acc_row_id"}', 
			'{
			"acc_last_upd", "acc_atc_kpp", "acc_company_status", "acc_alias_name", "acc_location", "acc_email", "acc_main_fax_number", "acc_main_phone_number", "acc_name", "acc_master_id", "acc_partytypecode", "acc_partyuid", "acc_pershing_id", "acc_ou_type_cd", "acc_active_flag", "acc_bank", "acc_birthday", "acc_branch", "acc_broker_inst", "acc_x_calypso", "acc_certificate", "acc_certification_date", "acc_confirmation_level", "acc_confirmation_method", "acc_counterparty_inst", "acc_country_domicile", "acc_country_of_risk", "acc_create_sec_cus_det", "acc_date_first_contact", "acc_default_commission", "acc_x_diasoft", "acc_division_code", "acc_expiration_date", "acc_fsa_ref", "acc_family_status", "acc_x_fidessa", "acc_former_names", "acc_fund_id", "acc_inn", "acc_industry", "acc_institution", "acc_issued_by", "acc_issue_date", "acc_next_review_code", "acc_micex_client", "acc_micex_code", "acc_midas_number", "acc_market_info_5", "acc_par_ou_id", "acc_x_midas", "acc_name_in_english", "acc_oasys_code", "acc_parent", "acc_parent_name", "acc_passport_number", "acc_passport_seria", "acc_passport_type", "acc_price_accuracy", "acc_rts_by_in", "acc_rts_code", "acc_ready_for_export", "acc_registered_by", "acc_residence", "acc_swift_financial", "acc_swift_identifier", "acc_sex", "acc_tax", "acc_vtbc_adopted", "acc_vtbe_adopted", "acc_vtbcadditionaladoptions", "acc_ctm_code", "acc_emir_cntp_code", "acc_vtbc_mic_code", "acc_vtbi_adopted", "acc_swiss_midas", "acc_vtbt_adopted", "acc_hasaccounts", "acc_bookingpriceroundscale", "acc_bookingsettlepriceroundscale", "acc_fixclientsidentifiers", "acc_isgiveupbrokerflag", "acc_segment", "acc_more_types_jsonb", "acc_adoptions_jsonb", "acc_cut_address_jsonb"
			}') 
		as hashdiff_table(hashdiff bytea, effective_from timestamp, acc_row_id varchar)
	;

	drop table if exists q_delta
	;
    create temporary table q_delta on commit preserve rows as
		select 
			  "acc_last_upd", "acc_atc_kpp", "acc_company_status", "acc_alias_name", "acc_location", "acc_email", "acc_main_fax_number", "acc_main_phone_number", "acc_name", "acc_master_id", "acc_partytypecode", "acc_partyuid", "acc_pershing_id", "acc_ou_type_cd", "acc_active_flag", "acc_bank", "acc_birthday", "acc_branch", "acc_broker_inst", "acc_x_calypso", "acc_certificate", "acc_certification_date", "acc_confirmation_level", "acc_confirmation_method", "acc_counterparty_inst", "acc_country_domicile", "acc_country_of_risk", "acc_create_sec_cus_det", "acc_date_first_contact", "acc_default_commission", "acc_x_diasoft", "acc_division_code", "acc_expiration_date", "acc_fsa_ref", "acc_family_status", "acc_x_fidessa", "acc_former_names", "acc_fund_id", "acc_inn", "acc_industry", "acc_institution", "acc_issued_by", "acc_issue_date", "acc_next_review_code", "acc_micex_client", "acc_micex_code", "acc_midas_number", "acc_market_info_5", "acc_par_ou_id", "acc_x_midas", "acc_name_in_english", "acc_oasys_code", "acc_parent", "acc_parent_name", "acc_passport_number", "acc_passport_seria", "acc_passport_type", "acc_price_accuracy", "acc_rts_by_in", "acc_rts_code", "acc_ready_for_export", "acc_registered_by", "acc_residence", "acc_swift_financial", "acc_swift_identifier", "acc_sex", "acc_tax", "acc_vtbc_adopted", "acc_vtbe_adopted", "acc_vtbcadditionaladoptions", "acc_ctm_code", "acc_emir_cntp_code", "acc_vtbc_mic_code", "acc_vtbi_adopted", "acc_swiss_midas", "acc_vtbt_adopted", "acc_hasaccounts", "acc_bookingpriceroundscale", "acc_bookingsettlepriceroundscale", "acc_fixclientsidentifiers", "acc_isgiveupbrokerflag", "acc_segment", "acc_more_types_jsonb", "acc_adoptions_jsonb", "acc_cut_address_jsonb"
			, valid_from, valid_to
			, s.acc_row_id
			, effective_from
			, hashdiff
		from 
			(select distinct 
				  "acc_last_upd", "acc_atc_kpp", "acc_company_status", "acc_alias_name", "acc_location", "acc_email", "acc_main_fax_number", "acc_main_phone_number", "acc_name", "acc_master_id", "acc_partytypecode", "acc_partyuid", "acc_pershing_id", "acc_ou_type_cd", "acc_active_flag", "acc_bank", "acc_birthday", "acc_branch", "acc_broker_inst", "acc_x_calypso", "acc_certificate", "acc_certification_date", "acc_confirmation_level", "acc_confirmation_method", "acc_counterparty_inst", "acc_country_domicile", "acc_country_of_risk", "acc_create_sec_cus_det", "acc_date_first_contact", "acc_default_commission", "acc_x_diasoft", "acc_division_code", "acc_expiration_date", "acc_fsa_ref", "acc_family_status", "acc_x_fidessa", "acc_former_names", "acc_fund_id", "acc_inn", "acc_industry", "acc_institution", "acc_issued_by", "acc_issue_date", "acc_next_review_code", "acc_micex_client", "acc_micex_code", "acc_midas_number", "acc_market_info_5", "acc_par_ou_id", "acc_x_midas", "acc_name_in_english", "acc_oasys_code", "acc_parent", "acc_parent_name", "acc_passport_number", "acc_passport_seria", "acc_passport_type", "acc_price_accuracy", "acc_rts_by_in", "acc_rts_code", "acc_ready_for_export", "acc_registered_by", "acc_residence", "acc_swift_financial", "acc_swift_identifier", "acc_sex", "acc_tax", "acc_vtbc_adopted", "acc_vtbe_adopted", "acc_vtbcadditionaladoptions", "acc_ctm_code", "acc_emir_cntp_code", "acc_vtbc_mic_code", "acc_vtbi_adopted", "acc_swiss_midas", "acc_vtbt_adopted", "acc_hasaccounts", "acc_bookingpriceroundscale", "acc_bookingsettlepriceroundscale", "acc_fixclientsidentifiers", "acc_isgiveupbrokerflag", "acc_segment", "acc_more_types_jsonb", "acc_adoptions_jsonb", "acc_cut_address_jsonb"
				, acc_kafka_timestamp
				, acc_row_id
				from tt_queue) as s
		join 
		(
		select case when lag(effective_from) over(partition by acc_row_id order by effective_from) is null then '1900-01-01' else effective_from end as valid_from
				, coalesce(lead(effective_from) over(partition by acc_row_id order by effective_from) - interval '1 seconds','2999-01-01') as valid_to
				, acc_row_id
				, effective_from
				, hashdiff
			from(
				select min(effective_from) as effective_from
				 , acc_row_id
			     , hashdiff
			from(select *, count(*)filter(where is_changing)over w2 as sequence_of_duplicates
			     from(select *, (hashdiff)
			                   is distinct from
			                   lag((hashdiff)
			                        ,1,(hashdiff))
			                   over w1 as is_changing
			          from hashdiff_table as s0
			          window w1 as (partition by acc_row_id
			                        order by effective_from
			                        rows between unbounded preceding 
			                                 and unbounded following) ) as s1
			     window w2 as (partition by acc_row_id
			                   order by effective_from
			                   rows between unbounded preceding 
			                            and current row) ) as s2
			group by sequence_of_duplicates, acc_row_id, hashdiff
			) as s3
		) as s4
		on s.acc_row_id = s4.acc_row_id and to_timestamp(s.acc_kafka_timestamp/1000)::timestamp = s4.effective_from
	;

	insert into siebel.le_account_th
		(sys_load_ts, acc_row_id, "acc_last_upd", "acc_atc_kpp", "acc_company_status", "acc_alias_name", "acc_location", "acc_email", "acc_main_fax_number", "acc_main_phone_number", "acc_name", "acc_master_id", "acc_partytypecode", "acc_partyuid", "acc_pershing_id", "acc_ou_type_cd", "acc_active_flag", "acc_bank", "acc_birthday", "acc_branch", "acc_broker_inst", "acc_x_calypso", "acc_certificate", "acc_certification_date", "acc_confirmation_level", "acc_confirmation_method", "acc_counterparty_inst", "acc_country_domicile", "acc_country_of_risk", "acc_create_sec_cus_det", "acc_date_first_contact", "acc_default_commission", "acc_x_diasoft", "acc_division_code", "acc_expiration_date", "acc_fsa_ref", "acc_family_status", "acc_x_fidessa", "acc_former_names", "acc_fund_id", "acc_inn", "acc_industry", "acc_institution", "acc_issued_by", "acc_issue_date", "acc_next_review_code", "acc_micex_client", "acc_micex_code", "acc_midas_number", "acc_market_info_5", "acc_par_ou_id", "acc_x_midas", "acc_name_in_english", "acc_oasys_code", "acc_parent", "acc_parent_name", "acc_passport_number", "acc_passport_seria", "acc_passport_type", "acc_price_accuracy", "acc_rts_by_in", "acc_rts_code", "acc_ready_for_export", "acc_registered_by", "acc_residence", "acc_swift_financial", "acc_swift_identifier", "acc_sex", "acc_tax", "acc_vtbc_adopted", "acc_vtbe_adopted", "acc_vtbcadditionaladoptions", "acc_ctm_code", "acc_emir_cntp_code", "acc_vtbc_mic_code", "acc_vtbi_adopted", "acc_swiss_midas", "acc_vtbt_adopted", "acc_hasaccounts", "acc_bookingpriceroundscale", "acc_bookingsettlepriceroundscale", "acc_fixclientsidentifiers", "acc_isgiveupbrokerflag", "acc_segment", "acc_more_types_jsonb", "acc_adoptions_jsonb", "acc_cut_address_jsonb", acc_nifi_filename, acc_kafka_topic, acc_kafka_partition, acc_kafka_offset, acc_kafka_timestamp)
	select v_landing_ts, acc_row_id, "acc_last_upd", "acc_atc_kpp", "acc_company_status", "acc_alias_name", "acc_location", "acc_email", "acc_main_fax_number", "acc_main_phone_number", "acc_name", "acc_master_id", "acc_partytypecode", "acc_partyuid", "acc_pershing_id", "acc_ou_type_cd", "acc_active_flag", "acc_bank", "acc_birthday", "acc_branch", "acc_broker_inst", "acc_x_calypso", "acc_certificate", "acc_certification_date", "acc_confirmation_level", "acc_confirmation_method", "acc_counterparty_inst", "acc_country_domicile", "acc_country_of_risk", "acc_create_sec_cus_det", "acc_date_first_contact", "acc_default_commission", "acc_x_diasoft", "acc_division_code", "acc_expiration_date", "acc_fsa_ref", "acc_family_status", "acc_x_fidessa", "acc_former_names", "acc_fund_id", "acc_inn", "acc_industry", "acc_institution", "acc_issued_by", "acc_issue_date", "acc_next_review_code", "acc_micex_client", "acc_micex_code", "acc_midas_number", "acc_market_info_5", "acc_par_ou_id", "acc_x_midas", "acc_name_in_english", "acc_oasys_code", "acc_parent", "acc_parent_name", "acc_passport_number", "acc_passport_seria", "acc_passport_type", "acc_price_accuracy", "acc_rts_by_in", "acc_rts_code", "acc_ready_for_export", "acc_registered_by", "acc_residence", "acc_swift_financial", "acc_swift_identifier", "acc_sex", "acc_tax", "acc_vtbc_adopted", "acc_vtbe_adopted", "acc_vtbcadditionaladoptions", "acc_ctm_code", "acc_emir_cntp_code", "acc_vtbc_mic_code", "acc_vtbi_adopted", "acc_swiss_midas", "acc_vtbt_adopted", "acc_hasaccounts", "acc_bookingpriceroundscale", "acc_bookingsettlepriceroundscale", "acc_fixclientsidentifiers", "acc_isgiveupbrokerflag", "acc_segment", "acc_more_types_jsonb", "acc_adoptions_jsonb", "acc_cut_address_jsonb", acc_nifi_filename, acc_kafka_topic, acc_kafka_partition, acc_kafka_offset, acc_kafka_timestamp
	from tt_queue
	;

	drop table if exists check_existing_data
	;
    create temporary table check_existing_data on commit preserve rows as
		select q.acc_row_id, q.effective_from as valid_from, q.valid_to, case when hd.hashdiff = q.hashdiff then 1 else 0 end as is_equals
		from
			(select hd.*
			from ctrl.get_hashdiff_table('siebel.le_account', 
				'{"valid_to", "row_id"}', 
				'{
				"last_upd", "atc_kpp", "company_status", "alias_name", "location", "email", "main_fax_number", "main_phone_number", "name", "master_id", "partytypecode", "partyuid", "pershing_id", "ou_type_cd", "active_flag", "bank", "birthday", "branch", "broker_inst", "x_calypso", "certificate", "certification_date", "confirmation_level", "confirmation_method", "counterparty_inst", "country_domicile", "country_of_risk", "create_sec_cus_det", "date_first_contact", "default_commission", "x_diasoft", "division_code", "expiration_date", "fsa_ref", "family_status", "x_fidessa", "former_names", "fund_id", "inn", "industry", "institution", "issued_by", "issue_date", "next_review_code", "micex_client", "micex_code", "midas_number", "market_info_5", "par_ou_id", "x_midas", "name_in_english", "oasys_code", "parent", "parent_name", "passport_number", "passport_seria", "passport_type", "price_accuracy", "rts_by_in", "rts_code", "ready_for_export", "registered_by", "residence", "swift_financial", "swift_identifier", "sex", "tax", "vtbc_adopted", "vtbe_adopted", "vtbcadditionaladoptions", "ctm_code", "emir_cntp_code", "vtbc_mic_code", "vtbi_adopted", "swiss_midas", "vtbt_adopted", "hasaccounts", "bookingpriceroundscale", "bookingsettlepriceroundscale", "fixclientsidentifiers", "isgiveupbrokerflag", "segment", "more_types_jsonb", "adoptions_jsonb", "cut_address_jsonb"
				}')
			as hd(hashdiff bytea, valid_to timestamp, row_id varchar)
			where hd.valid_to::date = '2999-01-01'::date
			) as hd
		join (select * from q_delta where valid_from::date = '1900-01-01'::date) as q 
			on hd.row_id = q.acc_row_id --and q.valid_to != '2999-01-01'::date
	;

	update siebel.le_account as a
	set valid_to = case when e.is_equals = 1 then e.valid_to
						when e.is_equals = 0 then e.valid_from - interval '1 seconds'
				   end
	from check_existing_data as e
	where (1=1)
		and e.acc_row_id = a.row_id 
		and a.valid_to::date = '2999-01-01'
	;

	delete from q_delta as q
		using check_existing_data as e
	where (1=1)
		and e.acc_row_id = q.acc_row_id 
		and q.valid_from::date = '1900-01-01'::date 
		and e.is_equals = 1
	;

	update q_delta as a
		set valid_from = e.valid_from
	from check_existing_data as e 
	where (1=1)
		and e.acc_row_id = a.acc_row_id 
		and a.valid_to::date = '2999-01-01' 
		and e.is_equals = 0
	;

	insert into siebel.le_account
	(sys_load_ts, effective_from, valid_from, valid_to, 
	 row_id, last_upd, atc_kpp, company_status, alias_name, "location", email, main_fax_number, main_phone_number, "name", master_id, partytypecode, partyuid, pershing_id, ou_type_cd, active_flag, bank, birthday, branch, broker_inst, x_calypso, certificate, certification_date, confirmation_level, confirmation_method, counterparty_inst, country_domicile, country_of_risk, create_sec_cus_det, date_first_contact, default_commission, x_diasoft, division_code, expiration_date, fsa_ref, family_status, x_fidessa, former_names, fund_id, inn, industry, institution, issued_by, issue_date, next_review_code, micex_client, micex_code, midas_number, market_info_5, par_ou_id, x_midas, name_in_english, oasys_code, parent, parent_name, passport_number, passport_seria, passport_type, price_accuracy, rts_by_in, rts_code, ready_for_export, registered_by, residence, swift_financial, swift_identifier, sex, tax, vtbc_adopted, vtbe_adopted, vtbcadditionaladoptions, ctm_code, emir_cntp_code, vtbc_mic_code, vtbi_adopted, swiss_midas, vtbt_adopted, hasaccounts, bookingpriceroundscale, bookingsettlepriceroundscale, fixclientsidentifiers, isgiveupbrokerflag, segment, more_types_jsonb, adoptions_jsonb, cut_address_jsonb
	 )
	select v_landing_ts, effective_from, valid_from, valid_to, 
		acc_row_id, "acc_last_upd", "acc_atc_kpp", "acc_company_status", "acc_alias_name", "acc_location", "acc_email", "acc_main_fax_number", "acc_main_phone_number", "acc_name", "acc_master_id", "acc_partytypecode", "acc_partyuid", "acc_pershing_id", "acc_ou_type_cd", "acc_active_flag", "acc_bank", "acc_birthday", "acc_branch", "acc_broker_inst", "acc_x_calypso", "acc_certificate", "acc_certification_date", "acc_confirmation_level", "acc_confirmation_method", "acc_counterparty_inst", "acc_country_domicile", "acc_country_of_risk", "acc_create_sec_cus_det", "acc_date_first_contact", "acc_default_commission", "acc_x_diasoft", "acc_division_code", "acc_expiration_date", "acc_fsa_ref", "acc_family_status", "acc_x_fidessa", "acc_former_names", "acc_fund_id", "acc_inn", "acc_industry", "acc_institution", "acc_issued_by", "acc_issue_date", "acc_next_review_code", "acc_micex_client", "acc_micex_code", "acc_midas_number", "acc_market_info_5", "acc_par_ou_id", "acc_x_midas", "acc_name_in_english", "acc_oasys_code", "acc_parent", "acc_parent_name", "acc_passport_number", "acc_passport_seria", "acc_passport_type", "acc_price_accuracy", "acc_rts_by_in", "acc_rts_code", "acc_ready_for_export", "acc_registered_by", "acc_residence", "acc_swift_financial", "acc_swift_identifier", "acc_sex", "acc_tax", "acc_vtbc_adopted", "acc_vtbe_adopted", "acc_vtbcadditionaladoptions", "acc_ctm_code", "acc_emir_cntp_code", "acc_vtbc_mic_code", "acc_vtbi_adopted", "acc_swiss_midas", "acc_vtbt_adopted", "acc_hasaccounts", "acc_bookingpriceroundscale", "acc_bookingsettlepriceroundscale", "acc_fixclientsidentifiers", "acc_isgiveupbrokerflag", "acc_segment", "acc_more_types_jsonb", "acc_adoptions_jsonb", "acc_cut_address_jsonb"
	from q_delta
	;

	PERFORM ctrl.set_offset_landing(p_out_schema, p_out_table, null, v_max_tt_queue_ts)
	;
	
    --------------------------- end logging --------------------------
	  get diagnostics p_out_rn := row_count;
	
	  p_out_end_ts := timezone('utc-3'::text, clock_timestamp() );
	
	  --if (p_log) then perform ctrl.set_load_log(p_out_batch_id, p_out_mls_code, p_out_module, p_out_schema, p_out_table, p_out_start_ts, p_out_end_ts, p_out_rn, p_out_note);
	  --end if;
    ------------------------------------------------------------------

  end;
$function$
;
