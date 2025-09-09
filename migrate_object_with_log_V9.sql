-- Snowflake Procedure to Migrate Objects between Databases using JavaScript with Logging (V9)

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
-- CALL MIGRATE_OBJECT_WITH_LOG_V9('SOURCE_DB', 'TARGET_DB', 'SOURCE_SCHEMA', 'TARGET_SCHEMA', 'TABLE', 'MY_TABLE');

CREATE OR REPLACE PROCEDURE MIGRATE_OBJECT_WITH_LOG_V9(
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
var migration_status = "";
var result_message = "";
var user = "UNKNOWN";

// Get current user safely
try {
    var whoami = snowflake.createStatement({sqlText: "SELECT CURRENT_USER()"}).execute();
    whoami.next();
    user = whoami.getColumnValue(1);
} catch (err) {
    // If this fails, user remains "UNKNOWN"
}

try {
    // --- Main Migration Logic ---
    var sql_command;
    var object_type_upper = OBJECT_TYPE.toUpperCase();
    var source_full_name;

    if (object_type_upper === "PROCEDURE" || object_type_upper === "FUNCTION") {
        // For procedures/functions, the object name includes the signature and should not be quoted.
        source_full_name = `"${SOURCE_DATABASE}"."${SOURCE_SCHEMA}".${OBJECT_NAME}`;
    } else {
        // For all other object types, quoting the name is safer.
        source_full_name = `"${SOURCE_DATABASE}"."${SOURCE_SCHEMA}"."${OBJECT_NAME}"`;
    }

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

        case "TASK":
            var get_ddl_sql = `SELECT GET_DDL('TASK', '${source_full_name}');`;
            var stmt = snowflake.createStatement({sqlText: get_ddl_sql});
            var rs = stmt.execute();
            rs.next();
            var ddl = rs.getColumnValue(1);

            // For tasks, only change the task name itself by splitting the DDL at the 'AS' keyword.
            const as_keyword_regex = /\s+as\s+/i;
            const as_match = ddl.match(as_keyword_regex);

            if (!as_match) {
                throw new Error("Could not find 'AS' keyword in TASK definition.");
            }

            const as_index = as_match.index;
            const task_header = ddl.substring(0, as_index);
            const task_body = ddl.substring(as_index);

            // Perform global, case-insensitive replacements on the header ONLY.
            var db_search_regex = new RegExp(`\\b${SOURCE_DATABASE}\\b`, 'gi');
            var schema_search_regex = new RegExp(`\\b${SOURCE_SCHEMA}\\b`, 'gi');
            let fixed_header = task_header.replace(db_search_regex, TARGET_DATABASE);
            fixed_header = fixed_header.replace(schema_search_regex, TARGET_SCHEMA);

            // Reconstruct the full DDL with the untouched body.
            sql_command = fixed_header + task_body;
            break;

        case "VIEW":
        case "PROCEDURE":
        case "FUNCTION":
        case "SEQUENCE":
        case "FILE FORMAT":
        case "PIPE":
        case "STREAM":
            var get_ddl_sql = `SELECT GET_DDL('${object_type_upper}', '${source_full_name}');`;
            var stmt = snowflake.createStatement({sqlText: get_ddl_sql});
            var rs = stmt.execute();
            rs.next();
            var ddl = rs.getColumnValue(1);

            // Use case-insensitive, global regex to replace database and schema names
            var db_search_regex = new RegExp(`\\b${SOURCE_DATABASE}\\b`, 'gi');
            var new_ddl = ddl.replace(db_search_regex, TARGET_DATABASE);

            var schema_search_regex = new RegExp(`\\b${SOURCE_SCHEMA}\\b`, 'gi');
            new_ddl = new_ddl.replace(schema_search_regex, TARGET_SCHEMA);

            sql_command = new_ddl;
            break;

        default:
             throw new Error(`Object type '${OBJECT_TYPE}' not supported.`);
    }

    var final_stmt = snowflake.createStatement({sqlText: sql_command});
    final_stmt.execute();

    // If we reach here, it was successful
    migration_status = "COMPLETED SUCCESSFULLY";
    result_message = `Object '${OBJECT_NAME}' of type '${OBJECT_TYPE}' migrated successfully.`;

} catch (err) {
    // If any error occurs in the try block
    migration_status = "FAILURE";
    result_message = `Failed to migrate object '${OBJECT_NAME}'. Error: ${err.message}`;
}

// --- Final Logging Step ---
var log_table = `"${TARGET_DATABASE}"."UTILITIES"."cicd_log_table"`;
var log_error_message = (migration_status === "FAILURE") ? result_message : "";

var sql_insert_log = `
    INSERT INTO ${log_table} (
        TRIGGERED_BY, SOURCE_DATABASE, TARGET_DATABASE, SOURCE_SCHEMA, TARGET_SCHEMA,
        OBJECT_NAME, OBJECT_TYPE, MIGRATION_STATUS, ERROR_MESSAGE
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);`;

try {
    var stmt_insert_log = snowflake.createStatement({
        sqlText: sql_insert_log,
        binds: [user, SOURCE_DATABASE, TARGET_DATABASE, SOURCE_SCHEMA, TARGET_SCHEMA,
                OBJECT_NAME, OBJECT_TYPE, migration_status, log_error_message]
    });
    stmt_insert_log.execute();
} catch (log_err) {
    // If logging fails, append a warning to the result message
    result_message += ` (WARNING: Failed to write to log table: ${log_err.message})`;
}

return result_message;
$$;
