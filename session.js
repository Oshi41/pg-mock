const _ = require('lodash');
const {get_table_from_sql, get_value} = require('./executor_utils.js');
const {Parser, Select, Function, Expr, Insert_Replace, Delete, Drop, From, Column} = require('node-sql-parser');

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

            case 'delete':
                return this.#delete(parsed, args);

            case 'drop':
                return this.#drop(parsed, args);

            default:
                throw new Error(`Command ${parsed.type} unsupported yet`);
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
        const raw_from = this.#resolve_from(sql.from, args);
        let arr_source = this.#map_by_columns(raw_from, sql.columns, sql.from[0].table);

        if (sql.where) {
            arr_source = arr_source.filter(x => this.#check_where(x, sql.where, args));
        }

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
     * @returns {PgResult}
     */
    #insert(sql, args) {
        if (sql.type != 'insert')
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
        let rows = undefined;
        if (sql.returning) {
            switch (sql.returning.type) {
                case "returning":
                    if (Array.isArray(sql.returning.columns)) {
                        rows = this.#map_by_columns(to_insert, sql.returning.columns);
                    } else if (sql.returning.columns == '*') {
                        rows = to_insert;
                    } else {
                        throw new Error('Unsupported return value: ' + JSON.stringify(sql.returning, null, 2));
                    }
                    break;

                default:
                    throw new Error('Unknown returning type: ' + sql.returning.type);
            }
        }

        return {
            rows,
            rowCount: to_insert.length,
        };
    }

    /**
     * Delete rows
     * @param sql {Delete}
     * @param args {[]}
     * @returns {PgResult}
     */
    #delete(sql, args) {
        const arr_source = get_table_from_sql(this.#tables, sql);
        let to_delete = [];
        if (sql.where) {
            to_delete = arr_source.filter(x => this.#check_where(x, sql.where, args));
        }
        _.remove(arr_source, x => to_delete.includes(x));
        return {
            rowCount: to_delete.length,
        };
    }

    /**
     *
     * @param sql {Drop}
     * @param args {[]}
     * @returns {PgResult}
     */
    #drop(sql, args) {
        switch (sql.keyword) {
            case 'table':
                for (let {table} of sql.name)
                    this.#tables.delete(table);
                return {};

            default:
                throw new Error('Unknown drop keyword: ' + sql.keyword);
        }
    }

    // endregion

    // region helping methods

    /**
     * Map source items by passed columns expressions
     * @param arr {{[key: string]: any}[]} - selected sources. Can be from different tables.
     * Key = table name, value = data item
     * @param columns {Column[]}
     * @param default_table_name {string | null} - default table name to map
     */
    #map_by_columns(arr, columns, default_table_name = null) {
        if (!arr || !columns)
            return;
        if (!arr.length)
            return [];

        return arr.map(x => {
            const copy = {};

            for (let column of columns) {
                if (!column.expr)
                    throw new Error('Unsupported column: ' + JSON.stringify(column, null, 2));

                switch (column.expr?.type) {
                    case "column_ref":
                        const table_name = column.expr.table || default_table_name;
                        const source_item = _.get(x, table_name);
                        if (source_item) {
                            if (column.expr.column == '*') {
                                Object.assign(copy, source_item);
                            } else {
                                const key = column.expr.column;
                                copy[column.as || key] = source_item[key];
                            }
                        }
                        break;

                    default:
                        throw new Error('Unsupported column type: ' + column.expr.type);
                }
            }

            return copy;
        });
    }

    /**
     * Recursive function checking if data item matches currecct condition
     * @param item {any} - data item
     * @param exp {Expr | Function} - SQL where clause
     * @param args {[]} - sql args
     * @returns {boolean}
     */
    #check_where(item, exp, args) {
        switch (exp.type) {
            case 'binary_expr':
                if ('AND OR'.split(' ').includes(exp.operator)) {
                    const sides = [exp.left, exp.right];
                    return exp.operator == 'AND'
                        ? sides.every(x => this.#check_where(item, x, args))
                        : sides.some(x => this.#check_where(item, x, args));
                } else {
                    const left = get_value(item, exp.left, args);
                    const right = get_value(item, exp.right, args);
                    return this.#check_binary_exp(left, right, exp.operator, args);
                }

            default:
                throw new Error('Unsupported "where" expression type: ' + exp.type);
        }
    }

    #check_binary_exp(left, right, operator, args) {
        switch (operator) {
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
                if (operator.includes('NOT'))
                    like_result = !like_result;
                return like_result;

            case 'NOT BETWEEN':
            case 'BETWEEN':
                const from = get_value(null, right[0], args);
                const to = get_value(null, right[1], args);
                let between_result = from <= left && left <= to;
                if (operator.includes('NOT'))
                    between_result = !between_result;
                return between_result;

            default:
                throw new Error('Unsupported binary operator: ' + operator);
        }
    }

    /**
     * @param froms {From[]}
     */
    #check_from_and_throw(froms) {
        for (let from of froms) {
            if (!from.table)
                throw new Error('Unsupported from: ' + JSON.stringify(from, null, 2));

            if (from.join) {
                if (from.on.type != 'binary_expr')
                    throw new Error('Unknown join on expression: ' + JSON.stringify(from, null, 2));

                const {on: {left, right}} = from;
                const operators = [left, right];
                while (operators.length) {
                    const op = operators.shift();
                    if (op.type == 'column_ref')
                        continue;

                    if (op.type == 'binary_expr') {
                        const tables = new Set();
                        const inner_operators = [op.left, op.right];

                        for (let i = 0; i < inner_operators.length; i++) {
                            const current = inner_operators[i];
                            if (current.type == 'binary_expr')
                                inner_operators.push(current.left, current.right);
                            if (current.type == 'column_ref') {
                                tables.add(current.table);
                                continue;
                            }
                        }

                        // join must concat this table
                        if (!tables.has(from.table))
                            throw new Error('Unknown join on expression: ' + JSON.stringify(from, null, 2));

                        operators.push(...inner_operators);
                        continue;
                    }

                    // any value constraint
                    if (op.hasOwnProperty('value'))
                        continue;

                    throw new Error('Unknown join on expression: ' + JSON.stringify(from, null, 2));
                }
            }
        }

        // checking joins actually
        if (froms.length > 1) {
            if (froms[0].join)
                throw new Error('Invalid from expression: ' + JSON.stringify(froms, null, 2));

            for (let i = 1; i < froms.length; i++) {
                const from = froms[i];
                if (!from.join)
                    throw new Error('Invalid from expression: ' + JSON.stringify(froms, null, 2));

                const tables_in_join = [from.on.left.table, from.on.right.table];
                if (!tables_in_join.includes(froms[i].table) && tables_in_join.includes(froms[i - 1].table))
                    throw new Error('Invalid join mapping: ' + JSON.stringify(froms, null, 2));
            }
        }
    }

    /**
     * Resolves from request, respecting joins
     * @param arr_from {From[]}
     * @param args {[]}
     * @returns {({[key: string]: any})[]}
     */
    #resolve_from(arr_from, args) {
        this.#check_from_and_throw(arr_from);

        if (arr_from.length == 1)
            return this.#tables.get(arr_from[0].table)?.map(x => ({[arr_from[0].table]: x})) || [];

        /**
         * @type {{[key: string]: any}[][]}
         */
        const raw_results = [];

        for (let i = 1; i < arr_from.length; i++) {
            const from = arr_from[i];
            const {left, right, operator, type} = from.on;

            if (type == 'binary_expr') {

            }

            // TODO: support binary_expr

            const left_collection = this.#tables.get(left.table);
            const right_collection = this.#tables.get(right.table);

            let merged = [];

            for (let left_el of left_collection) {
                const mapped_right = right_collection.find(x => this.#check_binary_exp(left_el[left.column], x[right.column], operator, args));
                merged.push({left: left_el, right: mapped_right});
            }

            for (let right_el of right_collection) {
                if (!merged.find(x => x.right == right_el)) {
                    const mapped_left = left_collection.find(x => this.#check_binary_exp(x[left.column], right_el[right.column], operator, args));
                    merged.push({left: mapped_left, right: right_el});
                }
            }

            let join_result;

            switch (from.join) {
                case 'INNER JOIN':
                    join_result = merged.filter(x => x.left && x.right);
                    break;

                case 'LEFT JOIN':
                    join_result = merged.filter(x => x.left);
                    break;

                case 'RIGHT JOIN':
                    join_result = merged.filter(x => x.right);
                    break;

                case 'CROSS JOIN':
                case 'FULL JOIN':
                    join_result = merged;
                    break;

                default:
                    throw new Error('Unknonwn join function: ' + from.join);
            }

            merged = merged.map(x => ({
                ...x.left && {[left.table]: x.left},
                ...x.right && {[right.table]: x.right},
            }));

            raw_results.push(merged);
        }

        const compressed = [];

        for (let per_table of raw_results) {
            for (let mapped_value of per_table) {
                const keys = Object.keys(mapped_value);
                let existing = compressed.find(x => _.isEqual(_.pick(x, keys), mapped_value));
                if (!existing)
                    compressed.push(existing = mapped_value);
                else
                    Object.assign(existing, mapped_value);
            }
        }

        return compressed;
    }

    // endregion
}

module.exports = Session;