const _ = require('lodash');
const {get_table_from_sql, get_value} = require('./executor_utils.js');
const {Parser, Select, Function, Expr, Insert_Replace} = require('node-sql-parser');

/**
 * @typedef {object} PgResult
 * @property rows {[]} - returned rows
 * @property rowCount {number} - how much rows were affected
 */

class Session {
    #parser = new Parser();

    /**
     * Inner tables
     * @type {Map<string, []>}
     */
    #tables = new Map();
    /**
     *
     * @type {{sql: string, args: []}[]}
     */
    #history = [];

    /**
     * Queries data
     * @param sql {string}
     * @param args {[]}
     * @returns {Promise<PgResult>}
     */
    async query(sql, args) {
        this.#history.push({sql, args});

        const parsed = this.#parser.astify(sql, {database: 'PostgresQL'});

        switch (parsed.type) {
            case 'select':
                return this.#select(parsed, args);

            case 'insert':
                return this.#insert(parsed, args);
        }
    }

    get history() {
        return this.#history;
    }

    get tables() {
        return this.#tables;
    }

    // region Executors

    /**
     * Handle select query
     * @param sql {Select}
     * @param args {[]}
     * @returns {PgResult}
     */
    #select(sql, args) {
        let arr_source = get_table_from_sql(this.#tables, sql);

        if (sql.where) {
            /**
             * Recursive function checking if data item matches currecct condition
             * @param item {any} - data item
             * @param exp {Expr | Function} - SQL where clause
             */
            function check_where(item, exp) {
                switch (exp.type) {
                    case 'binary_expr':
                        if ('AND OR'.split(' ').includes(exp.operator)) {
                            const sides = [exp.left, exp.right];
                            return exp.operator == 'AND'
                                ? sides.every(x => check_where(item, x))
                                : sides.some(x => check_where(item, x));
                        } else {
                            const left = get_value(item, exp.left, args);
                            const right = get_value(item, exp.right, args);

                            switch (exp.operator) {
                                case '=':
                                    return left == right;
                                case '<>':
                                    return left != right;
                                case '>':
                                    return left > right;
                                case '<':
                                    return left < right;
                                case '<=':
                                case '=<':
                                    return left <= right;
                                case '>=':
                                case '=>':
                                    return left >= right;

                                case 'LIKE':
                                case 'NOT LIKE':
                                    const l_str = ('' + left).toLowerCase();
                                    const r_str = ('' + right).toLowerCase();
                                    let like_result = l_str.includes(r_str) || r_str.includes(l_str);
                                    if (exp.operator.includes('NOT'))
                                        like_result = !like_result;
                                    return like_result;

                                case 'NOT BETWEEN':
                                case 'BETWEEN':
                                    const from = get_value(item, right[0], args);
                                    const to = get_value(item, right[1], args);
                                    let between_result =  from <= left && left <= to;
                                    if (exp.operator.includes('NOT'))
                                        between_result = !between_result;
                                    return between_result;

                                default:
                                    throw new Error('Unsupported binary operator: ' + exp.operator);
                            }
                        }

                    default:
                        throw new Error('Unsupported "where" expression type: ' + exp.type);
                }
            }

            arr_source = arr_source.filter(x => check_where(x, sql.where));
        }

        arr_source = arr_source.map(x => {
            const copy = {};

            for (let column of sql.columns) {
                if (!column.expr)
                    throw new Error('Unsupported column: ' + JSON.stringify(column, null, 2));

                switch (column.expr?.type) {
                    case "column_ref":
                        if (column.expr.column == '*')
                            Object.assign(copy, x);
                        else {
                            const key = column.expr.column;
                            copy[column.as || key] = x[key];
                        }
                        break;

                    default:
                        throw new Error('Unsupported column type: ' + column.expr.type);
                }
            }

            return copy;
        });

        const orderby = sql.orderby || sql._orderby;
        if (orderby) {
            const columns = orderby.map(x => get_value(null, x.expr, args));
            const order = orderby.map(x => x.type.toLowerCase());
            arr_source = _.orderBy(arr_source, columns, order);
        }

        const limit = sql.limit || sql._limit;
        if (limit) {
            const to = +get_value(null, limit.value[0], args) || arr_source.length;
            const from = +get_value(null, limit.value[1], args) || 0;
            arr_source = arr_source.slice(from, from + to);
        }

        return {
            rows: arr_source,
            rowCount: arr_source.length,
        };
    }

    /**
     * Handles insert query
     * @param sql {Insert_Replace}
     * @param args
     */
    #insert(sql, args) {
        if (sql.type != 'replace')
            throw new Error('Use "#replace" instead');

        const arr_source = get_table_from_sql(this.#tables, sql);
        const to_insert = [];
        for (let insert_value of sql.values) {
            const obj = {};
            for (let i = 0; i < insert_value.value.length; i++) {
                const key = sql.columns[i];
                const value = get_value(null, insert_value.value[i], args);
                obj[key] = value;
            }
            if (!_.isEmpty(obj))
                to_insert.push(obj);
        }

        arr_source.push(...to_insert);
    }

    // endregion
}

module.exports = Session;