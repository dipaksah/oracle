create or replace PACKAGE PKG_CDIP_CLEAN_RUNID AS
    
    --type to collect list of tables
    TYPE type_tbl_string_list IS TABLE OF CLOB;

    --type to collect list of exceptional tables where schema_name exist instead of run_id
    TYPE type_tbl_string_list2 IS TABLE OF CLOB;

    --type to collect list of exceptional tables where runid exist instead of run_id
    TYPE type_tbl_string_list3 IS TABLE OF CLOB;

    --type to collect list of schemas
    TYPE type_schema_list IS TABLE OF CLOB INDEX BY PLS_INTEGER;

    -- Procedures to clean table records
    PROCEDURE SP_CDIP_CLEAN_RUNID;

END PKG_CDIP_CLEAN_RUNID;
/

create or replace PACKAGE BODY PKG_CDIP_CLEAN_RUNID AS

    -- Procedure to process multiple values and delete records from tables
    PROCEDURE SP_CDIP_CLEAN_RUNID AS
    
        -- Array type to store string values
        tbl_list type_tbl_string_list := type_tbl_string_list(
       'auto_audit_frequency_counts', 'auto_process_control',
        'auto_validation_rec_log','cdip_abandon_run_log','cdip_admin_update_source_schemas',
        'cdip_backup_requests','cdip_ccd_file_reln','cdip_cot','cdip_cot_file_reln',
        'cdip_data_load','cdip_data_load_custom','cdip_data_load_log','cdip_data_profiler_location',
        'cdip_extract_config','cdip_file_archive_log','cdip_import_export_run','cdip_imputed_claims',
        'cdip_imputed_claims_schemas','cdip_jira_tickets','cdip_lkp_hedis_counts','cdip_qc_automation_log',
        'cdip_qi_append_dst_run','cdip_qi_engine_job','cdip_qi_engine_job_log',
        'cdip_qi_process_run','cdip_qi_process_run_log','cdip_qi_profspool_log',
        'cdip_qi_supp_impact_report_run','cdip_reprocess_run_log','cdip_rolling_measures',
        'cdip_rolling_measures_log','cdip_run_checklist_status','cdip_run_config_issue','cdip_run_custom_extract_config',
        'cdip_run_custom_prelim_config','cdip_run_custom_script','cdip_run_hold_log','cdip_run_measure','cdip_run_signoff',
        'cdip_run_status','cdip_run_stgschema','cdip_schema_data_transfer_config','cdip_schema_data_transfer_log',
        'cdip_star_file_reln','cdip_run_measure','cdip_run_config');

        --Exceptional case where schema_name exist instead of run_id
        tbl_list2 type_tbl_string_list2 := type_tbl_string_list2(
        'aip_schema_details','cdip_schema_table_log'
        );

        --Exceptional case where runid exist instead of run_id
        tbl_list3 type_tbl_string_list3 := type_tbl_string_list3(
        'cdip_qi_log_hum_merges','cdip_qi_processlog_gen'
        );

        -- Collection to store the schems list
        l_schema_names type_schema_list;  

        --local variables
        l_table     CLOB;
        l_table2    CLOB;
        l_table3    CLOB;
        l_query     CLOB;

        l_start     PLS_INTEGER := 1;
        l_limit     PLS_INTEGER := 20;
        l_tbl_values CLOB;

        CURSOR c_runids IS
        SELECT DISTINCT
            target_schema
        FROM
            cdip_run_config@aipdb
        WHERE
            target_schema IS NOT NULL
            AND target_schema NOT IN (
                SELECT
                    username
                FROM
                    all_users
                WHERE
                    username IS NOT NULL
                UNION
                SELECT
                    username
                FROM
                    all_users@mndevora01_pdb
                WHERE
                    username IS NOT NULL
            );

    BEGIN

        --First loop for table list 1
        -- Loop through each value for RUN_ID field 
        FOR i IN 1..tbl_list.COUNT LOOP

            l_table := tbl_list(i); 

            OPEN c_runids;
            LOOP
                FETCH c_runids BULK COLLECT INTO l_schema_names LIMIT l_limit;
                EXIT WHEN l_schema_names.COUNT = 0;

                --Construct comma separeted values for IN clause
                l_tbl_values := NULL;
                FOR j IN 1..l_schema_names.COUNT 
                LOOP
                    IF j > 0 THEN
                        l_tbl_values := l_tbl_values || ',';
                    END IF;
                    l_tbl_values := l_tbl_values||''''||l_schema_names(j)||'''';
                END LOOP;

                -- Delete schema records for each table one by one where run_id field exist
                 l_query := '
                            DELETE FROM aipdb.'||l_table||'
                            WHERE
                                run_id IN (
                                    '||LTRIM(l_tbl_values,',')||'
                                )';
                 EXECUTE IMMEDIATE l_query;
                  --DBMS_OUTPUT.PUT_LINE(l_query);
             END LOOP;
             CLOSE c_runids;

         END LOOP;

        --Second loop for table list 2
        -- Loop through each value for RUN_ID field 
        FOR i IN 1..tbl_list2.COUNT LOOP

            l_table2 := tbl_list2(i);

            OPEN c_runids;
            LOOP
                FETCH c_runids BULK COLLECT INTO l_schema_names LIMIT l_limit;
                EXIT WHEN l_schema_names.COUNT = 0;

                --Construct comma separeted values for IN clause
                l_tbl_values := NULL;
                FOR j IN 1..l_schema_names.COUNT 
                LOOP
                    IF j > 0 THEN
                        l_tbl_values := l_tbl_values || ',';
                    END IF;
                    l_tbl_values := l_tbl_values||''''||l_schema_names(j)||'''';
                END LOOP;

                -- Delete schema records for each table one by one where run_id field exist
                 l_query := '
                            DELETE FROM aipdb.'||l_table2||'
                            WHERE
                                schema_name IN (
                                    '||LTRIM(l_tbl_values,',')||'
                                )';
                 EXECUTE IMMEDIATE l_query;
                  --DBMS_OUTPUT.PUT_LINE(l_query);
             END LOOP;
             CLOSE c_runids;

         END LOOP;

         --Third loop for table list 3
        -- Loop through each value for RUN_ID field 
        FOR i IN 1..tbl_list3.COUNT LOOP

            l_table3 := tbl_list3(i); 

            OPEN c_runids;
            LOOP
                FETCH c_runids BULK COLLECT INTO l_schema_names LIMIT l_limit;
                EXIT WHEN l_schema_names.COUNT = 0;

                --Construct comma separeted values for IN clause
                l_tbl_values := NULL;
                FOR j IN 1..l_schema_names.COUNT 
                LOOP
                    IF j > 0 THEN
                        l_tbl_values := l_tbl_values || ',';
                    END IF;
                    l_tbl_values := l_tbl_values||''''||l_schema_names(j)||'''';
                END LOOP;

                -- Delete schema records for each table one by one where run_id field exist
                 l_query := '
                            DELETE FROM aipdb.'||l_table3||'
                            WHERE
                                runid IN (
                                    '||LTRIM(l_tbl_values,',')||'
                                )';
                 EXECUTE IMMEDIATE l_query;
                 -- DBMS_OUTPUT.PUT_LINE(l_query);
             END LOOP;
             CLOSE c_runids;

         END LOOP;

        COMMIT;

        EXCEPTION 
            WHEN OTHERS THEN 
            ROLLBACK;        
    END SP_CDIP_CLEAN_RUNID;

END PKG_CDIP_CLEAN_RUNID;
/