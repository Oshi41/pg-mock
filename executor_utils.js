const {AST, ColumnRef, Param, Value} = require('node-sql-parser');

/**
 * Resolves actual table from SQL query
 * @param map {Map<string, []>}
 * @param sql {AST | {table: string}}
 * @returns {[]}
 */
function get_table_from_sql(map, sql) {
    const table = find_table(sql);
    const array = map.get(table) || [];
    if (!map.has(table))
        map.set(table, array);
    return array;
}

/**
 * Returns table from SQL query
 * @param sql {AST | {table: string}}
 */
function find_table(sql) {
    if (typeof sql.table == 'string')
        return sql.table;

    switch (sql.type) {
        case "drop":
            return sql.name[0];

        case "insert":
        // TODO debug
        case "create":
        case "delete":
        case "replace":
        case "update":
        case 'alter':
            return sql.table[0].table;

        case "select":
            return sql.from[0].table;

        default:
            throw new Error('Cannot get table from sql query');
    }
}

/**
 * Retreives actual value from SQL expression
 * @param item {any} - Data item
 * @param exp {ColumnRef | Param | Value} - SQL expression
 * @param args {[]} - sql arguments
 * @returns {string | number | boolean}
 */
function get_value(item, exp, args) {
    if (!exp)
        return;

    switch (exp.type) {
        case 'column_ref':
            return item[exp.column];

        case 'var':
            return resolve_sql_param(exp, args);

        default:
            return exp.value;
    }
}

/**
 * Returns parameter resolved value
 * @param exp {{type: string, name: string}}
 * @param args {[]}
 * @returns {any | undefined}
 */
function resolve_sql_param(exp, args) {
    if (exp.type == 'var') {
        let index = (+exp.name - 1) || 0;
        return args[index];
    }
}

module.exports = {
    get_table_from_sql,
    get_value,
};