-- Snowflake Procedure to Migrate Objects between Databases using JavaScript

-- Usage:
-- CALL MIGRATE_OBJECT('SOURCE_DB', 'TARGET_DB', 'SOURCE_SCHEMA', 'TARGET_SCHEMA', 'TABLE', 'MY_TABLE');
-- CALL MIGRATE_OBJECT('SOURCE_DB', 'TARGET_DB', 'SOURCE_SCHEMA', 'TARGET_SCHEMA', 'VIEW', 'MY_VIEW');

CREATE OR REPLACE PROCEDURE MIGRATE_OBJECT(
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
try {
    var sql_command;
    var object_type_upper = OBJECT_TYPE.toUpperCase();
    var source_full_name = `"${SOURCE_DATABASE}"."${SOURCE_SCHEMA}"."${OBJECT_NAME}"`;
    var target_full_name = `"${TARGET_DATABASE}"."${TARGET_SCHEMA}"."${OBJECT_NAME}"`;
    var target_schema_full_name = `"${TARGET_DATABASE}"."${TARGET_SCHEMA}"`;

    // Check if target schema exists
    try {
        snowflake.execute({sqlText: `USE SCHEMA ${target_schema_full_name};`});
    } catch (err) {
        return `Failed to migrate object. Error: Target schema '${TARGET_DATABASE}.${TARGET_SCHEMA}' does not exist or is not accessible.`;
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

            // Perform a more robust replacement
            var search_string = `"${SOURCE_DATABASE}"."${SOURCE_SCHEMA}"`;
            var replace_string = `"${TARGET_DATABASE}"."${TARGET_SCHEMA}"`;
            var new_ddl = ddl.split(search_string).join(replace_string);

            // For objects that might not be fully qualified, also replace schema name
            var schema_search = `"${SOURCE_SCHEMA}"`
            var schema_replace = `"${TARGET_SCHEMA}"`
            new_ddl = new_ddl.split(schema_search).join(schema_replace)

            sql_command = new_ddl;
            break;

        default:
            return `Object type '${OBJECT_TYPE}' not supported. Supported types are: TABLE, VIEW, PROCEDURE, FUNCTION, TASK, SEQUENCE, FILE FORMAT, PIPE, STREAM.`;
    }

    var final_stmt = snowflake.createStatement({sqlText: sql_command});
    final_stmt.execute();

    return `Object '${OBJECT_NAME}' of type '${OBJECT_TYPE}' migrated successfully from '${SOURCE_DATABASE}'.'${SOURCE_SCHEMA}' to '${TARGET_DATABASE}'.'${TARGET_SCHEMA}'.`;

} catch (err) {
    return `Failed to migrate object '${OBJECT_NAME}' of type '${OBJECT_TYPE}'. Error: ${err.message}`;
}
$$;
