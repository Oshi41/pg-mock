const {ok} = require('assert');
const Session = require('./session.js');

class PgMock {
    /** @type {Session}*/
    #main_session;
    /** @type {Session}*/
    #transaction;

    async connect() {
        ok(!this.#main_session, 'You were already connected');
        this.#main_session = new Session();
        return true;
    }

    async end() {
        ok(!!this.#main_session, 'You were already disconnected');
        this.#main_session = null;
        return true;
    }

    /**
     * @param sql {string}
     * @param args {[]}
     * @returns {Promise<PgResult>}
     */
    async query(sql, args) {
        if (this.#transaction || 'begin commit rollback'.split(' ').includes(sql)) {
            return await this.#handle_transaction(sql, args);
        }

        if (this.#main_session)
            return await this.#main_session.query(sql, args);

        throw new Error('You should open connection');
    }

    /**
     * @param sql {'begin' | 'commit' | 'rollback' | string}
     * @param args
     * @returns {Promise}
     */
    async #handle_transaction(sql, args) {
        switch (sql) {
            case 'begin':
                if (this.#transaction)
                    throw new Error('Transaction was already started, use "commit" or "rollback"');
                this.#transaction = new Session();
                return true;

            case 'commit':
                if (!this.#transaction)
                    throw new Error('You should start transaction with "begin"');

                for (let historyElement of this.#transaction.history) {
                    await this.#main_session.query(historyElement.sql, historyElement.args);
                }

                this.#transaction = null;
                return true;

            case 'rollback':
                if (!this.#transaction)
                    throw new Error('You should start transaction with "begin"');

                this.#transaction = null;
                return true;

            default:
                if (!this.#transaction)
                    throw new Error('Transaction logic error!');

                return await this.#transaction.query(sql, args);
        }
    }

    /**
     * @returns {Map<string, []>}
     * @private
     */
    get _tests_only_table_map() {
        const session = this.#transaction || this.#main_session;
        return session.tables;
    }
}

module.exports = {PgMock, Session};
