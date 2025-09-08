-- Snowflake Procedure to Migrate Objects between Databases using JavaScript with Logging

-- Prerequisite: A log table. You can create it using the following DDL:
/*
CREATE SCHEMA IF NOT EXISTS UTILITIES;
CREATE TABLE IF NOT EXISTS UTILITIES.cicd_log_table (
    LOG_ID NUMBER AUTOINCREMENT,
    EVENT_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    TRIGGERED_BY VARCHAR,
    SOURCE_DATABASE VARCHAR,
    TARGET_DATABASE VARCHAR,
    SOURCE_SCHEMA VARCHAR,
    TARGET_SCHEMA VARCHAR,
    OBJECT_NAME VARCHAR,
    OBJECT_TYPE VARCHAR,
    MIGRATION_STATUS VARCHAR,
    ERROR_MESSAGE VARCHAR,
    PRIMARY KEY (LOG_ID)
);
*/

-- Usage:
-- CALL MIGRATE_OBJECT_WITH_LOG('SOURCE_DB', 'TARGET_DB', 'SOURCE_SCHEMA', 'TARGET_SCHEMA', 'TABLE', 'MY_TABLE');
-- CALL MIGRATE_OBJECT_WITH_LOG('SOURCE_DB', 'TARGET_DB', 'SOURCE_SCHEMA', 'TARGET_SCHEMA', 'VIEW', 'MY_VIEW');

CREATE OR REPLACE PROCEDURE MIGRATE_OBJECT_WITH_LOG(
    SOURCE_DATABASE VARCHAR,
    TARGET_DATABASE VARCHAR,
    SOURCE_SCHEMA VARCHAR,
    TARGET_SCHEMA VARCHAR,
    OBJECT_TYPE VARCHAR,
    OBJECT_NAME VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var log_table = `"${TARGET_DATABASE}"."UTILITIES"."cicd_log_table"`;
var log_schema = `"${TARGET_DATABASE}"."UTILITIES"`;
var log_id = -1;

// Function to log messages
function log(status, message = "") {
    var user = "";
    try {
        var whoami = snowflake.createStatement({sqlText: "SELECT CURRENT_USER()"}).execute();
        whoami.next();
        user = whoami.getColumnValue(1);
    } catch(err) {
        user = "UNKNOWN";
    }

    if (log_id === -1) {
        // Create log entry and get ID
        var sql_insert_log = `
            INSERT INTO ${log_table} (
                TRIGGERED_BY, SOURCE_DATABASE, TARGET_DATABASE, SOURCE_SCHEMA, TARGET_SCHEMA, OBJECT_NAME, OBJECT_TYPE, MIGRATION_STATUS, ERROR_MESSAGE
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?
            );`;
        var stmt_insert_log = snowflake.createStatement({
            sqlText: sql_insert_log,
            binds: [user, SOURCE_DATABASE, TARGET_DATABASE, SOURCE_SCHEMA, TARGET_SCHEMA, OBJECT_NAME, OBJECT_TYPE, status, message]
        });
        stmt_insert_log.execute();

        var get_log_id = snowflake.createStatement({sqlText: "SELECT last_query_id()"});
        var rs_log_id = get_log_id.execute();
        rs_log_id.next();
        var last_query_id = rs_log_id.getColumnValue(1);

        var get_log_pk = snowflake.createStatement({sqlText: `SELECT * FROM table(result_scan('${last_query_id}'))`});
        var rs_pk = get_log_pk.execute();
        rs_pk.next();
        log_id = rs_pk.getColumnValue(1);

    } else {
        // Update existing log entry
        var sql_update_log = `
            UPDATE ${log_table}
            SET MIGRATION_STATUS = ?, ERROR_MESSAGE = ?, EVENT_TIMESTAMP = CURRENT_TIMESTAMP()
            WHERE LOG_ID = ?;`;
        var stmt_update_log = snowflake.createStatement({
            sqlText: sql_update_log,
            binds: [status, message, log_id]
        });
        stmt_update_log.execute();
    }
}

try {
    log("IN_PROGRESS");

    var sql_command;
    var object_type_upper = OBJECT_TYPE.toUpperCase();
    var source_full_name = `"${SOURCE_DATABASE}"."${SOURCE_SCHEMA}"."${OBJECT_NAME}"`;
    var target_full_name = `"${TARGET_DATABASE}"."${TARGET_SCHEMA}"."${OBJECT_NAME}"`;
    var target_schema_full_name = `"${TARGET_DATABASE}"."${TARGET_SCHEMA}"`;

    // Check if target schema exists
    try {
        snowflake.execute({sqlText: `USE SCHEMA ${target_schema_full_name};`});
    } catch (err) {
        throw new Error(`Target schema '${TARGET_DATABASE}.${TARGET_SCHEMA}' does not exist or is not accessible.`);
    }

    switch (object_type_upper) {
        case "TABLE":
            sql_command = `CREATE OR REPLACE TABLE ${target_full_name} LIKE ${source_full_name};`;
            break;

        case "VIEW":
        case "PROCEDURE":
        case "FUNCTION":
        case "TASK":
        case "SEQUENCE":
        case "FILE FORMAT":
        case "PIPE":
        case "STREAM":
            var get_ddl_sql = `SELECT GET_DDL('${object_type_upper}', '${source_full_name}');`;
            var stmt = snowflake.createStatement({sqlText: get_ddl_sql});
            var rs = stmt.execute();
            rs.next();
            var ddl = rs.getColumnValue(1);

            var search_string = `"${SOURCE_DATABASE}"."${SOURCE_SCHEMA}"`;
            var replace_string = `"${TARGET_DATABASE}"."${TARGET_SCHEMA}"`;
            var new_ddl = ddl.split(search_string).join(replace_string);

            var schema_search = `"${SOURCE_SCHEMA}"`
            var schema_replace = `"${TARGET_SCHEMA}"`
            new_ddl = new_ddl.split(schema_search).join(schema_replace)

            sql_command = new_ddl;
            break;

        default:
             throw new Error(`Object type '${OBJECT_TYPE}' not supported. Supported types are: TABLE, VIEW, PROCEDURE, FUNCTION, TASK, SEQUENCE, FILE FORMAT, PIPE, STREAM.`);
    }

    var final_stmt = snowflake.createStatement({sqlText: sql_command});
    final_stmt.execute();

    log("SUCCESS");
    return `Object '${OBJECT_NAME}' of type '${OBJECT_TYPE}' migrated successfully.`;

} catch (err) {
    var error_message = `Failed to migrate object '${OBJECT_NAME}'. Error: ${err.message}`;
    if (log_id !== -1) {
        log("FAILURE", error_message);
    }
    return error_message;
}
$$;
