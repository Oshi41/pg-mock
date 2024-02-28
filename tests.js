const _ = require('lodash');
const {ok} = require('assert');
const {PgMock} = require('./index.js');

describe('select', () => {
    const users = [
        {
            id: 1,
            name: 'John',
            surname: 'Doe',
            money: 0,
        },
        {
            id: 2,
            name: 'Richy',
            surname: 'Rich',
            money: 1_123_567,
        },
        {
            id: 3,
            name: 'Mister',
            surname: 'Poor',
            money: 100,
        },
        {
            id: 4,
            name: 'Regular',
            surname: 'One',
            money: 12_000,
        },
    ];
    /** @type {PgMock}*/
    let client;
    beforeEach(async () => {
        client = new PgMock();
        await client.connect();

        const map = client._tests_only_table_map;
        map.set('users', users);
    });

    it('works', async () => {
        const {rows} = await client.query('select * from users');
        ok(_.isEqual(rows, users));
    });
    it('limit', async () => {
        for (let i = 1; i < users.length; i++) {
            const {rows} = await client.query(`select * from users limit ${i}`);
            ok(_.isEqual(rows, users.slice(0, i)));
        }
    });
    it('[SQL params] limit', async () => {
        for (let i = 1; i < users.length; i++) {
            const {rows} = await client.query('select * from users limit $1', [i]);
            ok(_.isEqual(rows, users.slice(0, i)));
        }
    });
    it('limit with offset', async () => {
        const {rows} = await client.query('select * from users limit 2 offset 1');
        ok(_.isEqual(rows, users.slice(1, 3)))
    });
    it('[SQL params] limit with offset', async () => {
        const {rows} = await client.query('select * from users limit $2 offset $1',
            [
                1, // offset
                2, // limit
            ]);
        ok(_.isEqual(rows, users.slice(1, 3)))
    });
    it('select partial fields', async () => {
        const fields = 'id name surname money'.split(' ');
        for (let i = 1; i < fields.length; i++) {
            let selected_fields = fields.slice(0, i);
            const {rows} = await client.query(`select ${selected_fields.join(', ')} from users`);
            const slice = users.map(x => _.pick(x, selected_fields));
            ok(_.isEqual(rows, slice));
        }
    });
    it('select partial fields and rename', async () => {
        const {rows} = await client.query('select id as _id, name as _name, surname as _surname, money as "bank account" from users');
        const slice = users.map(x => ({
            _id: x.id,
            _name: x.name,
            _surname: x.surname,
            'bank account': x.money,
        }));
        ok(_.isEqual(rows, slice));
    });
    it('[SQL params] select where >', async () => {
        const {rows} = await client.query('select * from users where money > $1', [0]);
        const slice = users.filter(x => x.money > 0);
        ok(_.isEqual(rows, slice));
    });
    it('[SQL params] select where <', async () => {
        const {rows} = await client.query('select * from users where money < $1', [12_000]);
        const slice = users.filter(x => x.money < 12_000);
        ok(_.isEqual(rows, slice));
    });
    it('[SQL params] select where =', async () => {
        const {rows} = await client.query('select * from users where money = $1', [100]);
        const slice = users.filter(x => x.money == 100);
        ok(_.isEqual(rows, slice));
    });
    it('[SQL params] select where <>', async () => {
        const {rows} = await client.query('select * from users where money <> $1', [100]);
        const slice = users.filter(x => x.money != 100);
        ok(_.isEqual(rows, slice));
    });
    it('[SQL params] select where LIKE', async () => {
        const {rows} = await client.query('select * from users where money LIKE $1', [567]);
        const slice = users.filter(x => ('' + x.money).includes('567'));
        ok(_.isEqual(rows, slice));
    });
    it('[SQL params] select where NOT LIKE', async () => {
        const {rows} = await client.query('select * from users where money not like $1', [567]);
        const slice = users.filter(x => !('' + x.money).includes('567'));
        ok(_.isEqual(rows, slice));
    });
    it('[SQL params] select where BETWEEN', async () => {
        const {rows} = await client.query('select * from users where money between $1 and $2', [10, 500]);
        const slice = users.filter(x => 10 <= x.money && x.money <= 500);
        ok(_.isEqual(rows, slice));
    });
    it('[SQL params] select where NOT BETWEEN', async () => {
        const {rows} = await client.query('select * from users where money not between $1 and $2', [10, 500]);
        const slice = users.filter(x => !(10 <= x.money && x.money <= 500));
        ok(_.isEqual(rows, slice));
    });
    it('[SQL params] select where AND', async () => {
        const {rows} = await client.query('select * from users where money > $1 and money < $2', [10, 500]);
        const slice = users.filter(x => x.money > 10 && x.money < 500);
        ok(_.isEqual(rows, slice));
    });
    it('[SQL params] select where OR', async () => {
        const {rows} = await client.query('select * from users where money > $1 or name = $2', [10, 'Richy']);
        const slice = users.filter(x => x.money > 10 || x.name == 'Richy');
        ok(_.isEqual(rows, slice));
    });
});