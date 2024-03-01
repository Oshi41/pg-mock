process.env.TESTS = 'true';

const _ = require('lodash');
const {ok, deepEqual} = require('assert');
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


    describe('join', () => {
        const history = [
            {
                client_id: 2,
                name: 'Computer',
                price: 6_000,
            },
            {
                client_id: 2,
                name: 'Car',
                price: 24_000,
            },
            {
                client_id: 3,
                name: 'Bread',
                price: 4,
            },
            {
                client_id: 4,
                name: 'Restaurant',
                price: 123,
            },
            {
                client_id: 2,
                name: 'House',
                price: 100_000,
            },
        ];
        beforeEach(()=>{
            const map = client._tests_only_table_map;
            map.set('history', history);
        });

        it('works', async ()=>{
           const {rows} = await client.query(`select users.name as name, history.name as product, price from history
                                                join users on history.client_id = users.id
                                                where price > 6000`);
           const expect = history.filter(x=>x.price > 6000).map(x=>({
               name: users.find(u=>u.id == x.client_id).name,
               product: x.name,
               price: x.price,
           }));
           deepEqual(rows, expect);
        });
        it('works with and', async ()=>{
            const {rows} = await client.query(`select users.name, users.surname from users
                                                    join history on history.client_id = users.id and history.price > 2000`);

            const expect = history.filter(x=>x.price > 6000).map(x=>({
                name: users.find(u=>u.id == x.client_id).name,
                product: x.name,
                price: x.price,
            }));
        });
    });
});

describe('insert', () => {
    /** @type {PgMock}*/
    let client;
    beforeEach(async () => {
        client = new PgMock();
        await client.connect();
    });

    async function check_insertion(expected, sql, args) {
        await client.query(sql, args);
        const rows = client._tests_only_table_map.get('users');
        ok(_.isEqual(expected, rows));
    }

    async function check_insertion_result(expected, sql, args) {
        const {rows} = await client.query(sql, args);
        ok(_.isEqual(expected, rows));
    }

    it('works', () => check_insertion(
        [{id: 0, name: 'John', surname: 'Doe', money: 100}],
        'insert into users (id, name, surname, money) values (0, "John", "Doe", 100)'
    ));
    it('[SQL params] multiple insertion', () => check_insertion(
        [
            {id: 0, name: 'John0', surname: 'Doe0', money: 10},
            {id: 1, name: 'John1', surname: 'Doe1', money: 100},
            {id: 2, name: 'John2', surname: 'Doe2', money: 1000},
        ],
        `insert into users (id, name, surname, money) values 
                (0, "John0", "Doe0", $1),
                (1, "John1", "Doe1", $2),
                (2, "John2", "Doe2", $3)
                `,
        [10, 100, 1000]
    ));
    it('[SQL params] multiple insertion retuning id', () => check_insertion_result(
        [
            {id: 0,},
            {id: 1,},
            {id: 2,},
        ],
        `insert into users (id, name, surname, money) values 
                (0, "John0", "Doe0", $1),
                (1, "John1", "Doe1", $2),
                (2, "John2", "Doe2", $3)
                returning id`,
        [10, 100, 1000]
    ));
    it('[SQL params] multiple insertion retuning *', () => check_insertion_result(
        [
            {NUMBER: 0, NAME: 'John0',},
            {NUMBER: 1, NAME: 'John1',},
            {NUMBER: 2, NAME: 'John2',},
        ],
        `insert into users (id, name, surname, money) values 
                (0, "John0", "Doe0", $1),
                (1, "John1", "Doe1", $2),
                (2, "John2", "Doe2", $3)
                returning id as NUMBER, name as NAME`,
        [10, 100, 1000]
    ));
});

describe('delete', () => {
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
    it('[SQL params] works', async () => {
        let expected = [...users];
        await client.query('delete from users where money > $1', [100]);
        _.remove(expected, x => x.money > 100);
        const rows = client._tests_only_table_map.get('users');
        ok(_.isEqual(rows, expected));
    });
});

describe('drop', () => {
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
        await client.query('drop table users');
        deepEqual(client._tests_only_table_map.get('users'), undefined);
    });

    it('multiple tables', async () => {
        client._tests_only_table_map.set('clients', [
            {
                id: 1,
                name: 'Client'
            }
        ]);
        await client.query('drop table users, clients');
        deepEqual(client._tests_only_table_map.get('users'), undefined);
        deepEqual(client._tests_only_table_map.get('clients'), undefined);
    });
});