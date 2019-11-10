use cql::compression::{Compression, Compressor};
use cql::def::*;
use cql::frame::Frame;
use cql::message::*;
use cql::request::*;
use cql::request::batch::*;
use cql::request::query::QueryParams;
use cql::response::error::*;
use cql::response::result::*;
use cql::types::*;

use strum::IntoEnumIterator;

use std::net::TcpStream;

use common::*;
use cql::request::batch::BatchQuery;

mod common;
mod fake_snappy;
mod snappy;

fn new_compressor(compression: Compression, fake: bool) -> Box<dyn Compressor> {
    match compression {
        Compression::Snappy => {
            if fake {
                Box::new(fake_snappy::Snappy::new())
            } else {
                Box::new(snappy::Snappy::new())
            }
        },
        Compression::Lz4 => unimplemented!("{:?}", compression),
    }
}

#[derive(Default)]
struct Builder {
    v: Version,
    stream_id: i16,
    compression: Option<Compression>,
    fake_compression: bool,
    ip: &'static str,
    port: u16,
}

impl Builder {
    fn new() -> Self {
        Default::default()
    }

    fn version(mut self, v: Version) -> Self {
        self.v = v;
        self
    }

    fn stream_id(mut self, stream_id: i16) -> Self {
        self.stream_id = stream_id;
        self
    }

    fn ip(mut self, ip: &'static str) -> Self {
        self.ip = ip;
        self
    }

    fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    fn compression(mut self, compression: Compression) -> Self {
        self.compression = Some(compression);
        self
    }

    fn fake_compression(mut self) -> Self {
        self.fake_compression = false;
        self
    }

    fn build(self) -> Client {
        let stream = TcpStream::connect((self.ip, self.port)).unwrap();
        let mut startup = Startup::new();
        let mut compressor = None;
        if let Some(compression) = self.compression {
            startup.set_compression(compression);
            compressor = Some(new_compressor(compression, self.fake_compression));
        }

        Client {
            startup,
            frame: Frame::new(self.v, self.stream_id, stream, compressor),
            auth: false,
        }
    }
}

struct Client {
    startup: Startup,
    frame: Frame<TcpStream>,
    auth: bool,
}

impl Client {
    pub fn options(&mut self) {
        let options = Options::new();
        self.frame.encode(&options).unwrap();

        let (_, rsp) = self.frame.decode().unwrap();
        let supported = match rsp {
            MessageKind::Supported(m) => m,
            _ => unreachable!("{:?}", rsp),
        };
        println!("{}", supported);
    }

    pub fn auth(&mut self) {
        if self.auth {
            return;
        }

        self.startup();

        let mut auth_response = AuthResponse::new();
        auth_response.set_token(auth_response_token("cassandra", "cassandra"));
        self.frame.encode(&auth_response).unwrap();

        let (_, rsp) = self.frame.decode().unwrap();
        let auth_success = match rsp {
            MessageKind::AuthSuccess(m) => m,
            _ => unreachable!("{:?}", rsp),
        };
        println!("{}", auth_success);

        self.auth = true;
    }

    pub fn register(&mut self) {
        self.auth();

        let mut register = Register::new();
        register.set_events(&EventType::iter().collect::<Vec<EventType>>());
        self.frame.encode(&register).unwrap();

        let (_, rsp) = self.frame.decode().unwrap();
        let ready = match rsp {
            MessageKind::Ready(m) => m,
            _ => unreachable!("{:?}", rsp),
        };
        println!("{}", ready);
    }

    pub fn event(&mut self) {
        self.register();

        loop {
            let (stream_id, rsp) = self.frame.decode().unwrap();
            let event = match rsp {
                MessageKind::Event(m) => m,
                _ => unreachable!("{:?}", rsp),
            };
            println!("{}", event);

            assert_eq!(stream_id, -1);
        }
    }

    pub fn err_server_error(&mut self) {
        self.frame.encode(&self.startup).unwrap();
        let mut auth_response = AuthResponse::new();
        auth_response.set_token(auth_response_token("cassandra", "cassandra"));
        self.frame.encode(&auth_response).unwrap();

        let e = self.match_error();
        assert_eq!(e.code(), ErrorCode::ServerError);
        assert_eq!(*e.exception(), ExceptionKind::None);
    }

    pub fn err_protocol_error(&mut self) {
        self.startup();

        let startup = Startup::new();
        self.frame.encode(&startup).unwrap();

        let e = self.match_error();
        assert_eq!(e.code(), ErrorCode::ProtocolError);
        assert_eq!(*e.exception(), ExceptionKind::None);
    }

    pub fn err_authentication_error(&mut self) {
        self.startup();

        let mut auth_response = AuthResponse::new();
        auth_response.set_token(auth_response_token("a", "a"));
        self.frame.encode(&auth_response).unwrap();

        let e = self.match_error();
        assert_eq!(e.code(), ErrorCode::AuthenticationError);
        assert_eq!(*e.exception(), ExceptionKind::None);
    }

    pub fn err_syntax_error(&mut self) {
        self.auth();

        let query = Query::from("abcdefg");
        self.frame.encode(&query).unwrap();

        let e = self.match_error();
        assert_eq!(e.code(), ErrorCode::SyntaxError);
        assert_eq!(*e.exception(), ExceptionKind::None);
    }

    pub fn err_unauthorized(&mut self) {
        self.auth();

        let query = Query::from("DROP KEYSPACE system");
        self.frame.encode(&query).unwrap();

        let e = self.match_error();
        assert_eq!(e.code(), ErrorCode::Unauthorized);
        assert_eq!(*e.exception(), ExceptionKind::None);
    }

    pub fn err_invalid(&mut self) {
        self.auth();

        let query = Query::from("SELECT * FROM a");
        self.frame.encode(&query).unwrap();

        let e = self.match_error();
        assert_eq!(e.code(), ErrorCode::Invalid);
        assert_eq!(*e.exception(), ExceptionKind::None);
    }

    pub fn err_config_error(&mut self) {
        self.auth();

        let query = "CREATE KEYSPACE system_auth WITH replication = {'replication_factor':1}";
        let query = Query::from(query);
        self.frame.encode(&query).unwrap();

        let e = self.match_error();
        assert_eq!(e.code(), ErrorCode::ConfigError);
        assert_eq!(*e.exception(), ExceptionKind::None);
    }

    pub fn err_already_exists(&mut self) {
        self.auth();

        let s = "CREATE KEYSPACE system_auth WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};";
        let query = Query::from(s);
        self.frame.encode(&query).unwrap();

        let e = self.match_error();
        assert_eq!(e.code(), ErrorCode::AlreadyExists);
        match e.exception() {
            ExceptionKind::AlreadyExists(_) => (),
            _ => unreachable!("{:?}", e.exception()),
        }
    }

    pub fn err_unprepared(&mut self) {
        self.auth();

        let execute = Execute::from(vec![0], QueryParams::default());
        self.frame.encode(&execute).unwrap();

        let e = self.match_error();
        assert_eq!(e.code(), ErrorCode::Unprepared);
        match e.exception() {
            ExceptionKind::Unprepared(_) => (),
            _ => unreachable!("{:?}", e.exception()),
        }
    }

    pub fn query(&mut self, query: &Query) -> Result {
        self.auth();

        self.frame.encode(query).unwrap();
        let (_, rsp) = self.frame.decode().unwrap();
        let result = match rsp {
            MessageKind::Result(m) => m,
            _ => unreachable!("{:?}", rsp),
        };

        match result.body() {
            ResultBody::Rows(ref rows) => println!("{:#?}", rows),
            _ => println!("{}", result),
        }
        return result
    }

    pub fn prepare(&mut self, prepare: &Prepare) -> Prepared {
        self.auth();

        self.frame.encode(prepare).unwrap();
        let (_, rsp) = self.frame.decode().unwrap();
        let result = match rsp {
            MessageKind::Result(m) => m,
            _ => unreachable!("{:?}", rsp),
        };

        match result.into_body() {
            ResultBody::Prepared(prepared) => {
                println!("{:?}", prepared);
                return prepared;
            },
            _ => unreachable!(),
        }
    }

    pub fn execute(&mut self, execute: &Execute) {
        self.auth();

        self.frame.encode(execute).unwrap();
        let (_, rsp) = self.frame.decode().unwrap();
        let result = match rsp {
            MessageKind::Result(m) => m,
            _ => unreachable!("{:?}", rsp),
        };

        println!("{:#?}", result);
    }

    pub fn batch(&mut self, batch: &Batch) {
        self.auth();

        self.frame.encode(batch).unwrap();
        let (_, rsp) = self.frame.decode().unwrap();
        let result = match rsp {
            MessageKind::Result(m) => m,
            _ => unreachable!("{:?}", rsp),
        };

        println!("{:#?}", result);
    }

    fn startup(&mut self) {
        self.frame.encode(&self.startup).unwrap();
        let (_, rsp) = self.frame.decode().unwrap();
        let authenticate = match rsp {
            MessageKind::Authenticate(m) => m,
            _ => unreachable!("{:?}", rsp),
        };
        println!("{}", authenticate);
    }

    fn match_error(&mut self) -> Error {
        let (_, rsp) = self.frame.decode().unwrap();
        let error = match rsp {
            MessageKind::Error(m) => m,
            _ => unreachable!("{:?}", rsp),
        };
        println!("{}", error);
        error
    }
}

fn builder() -> Builder {
    Builder::new().version(Version::V5).ip("192.168.100.4").port(9042).stream_id(1)
}

fn client() -> Client {
    builder().build()
}

fn snappy_client() -> Client {
    builder().compression(Compression::Snappy).build()
}

fn fake_snappy_client() -> Client {
    builder().fake_compression().compression(Compression::Snappy).build()
}

#[test]
fn options() {
    let mut client = client();
    client.options();
}

#[test]
fn auth() {
    let mut client = client();
    client.auth();
}

#[test]
fn register() {
    let mut client = client();
    client.register();
}

#[ignore]
#[test]
fn event() {
    let mut client = client();
    client.event();
}

#[test]
fn err_server_error() {
    let mut client = fake_snappy_client();
    client.err_server_error();
}

#[test]
fn err_protocol_error() {
    let mut client = client();
    client.err_protocol_error();
}

#[test]
fn err_authentication_error() {
    let mut client = client();
    client.err_authentication_error();
}

#[test]
fn err_syntax_error() {
    let mut client = client();
    client.err_syntax_error();
}

#[test]
fn err_unauthorized() {
    let mut client = client();
    client.err_unauthorized();
}

#[test]
fn err_invalid() {
    let mut client = client();
    client.err_invalid();
}

#[test]
fn err_config_error() {
    let mut client = client();
    client.err_config_error();
}

#[test]
fn err_already_exists() {
    let mut client = client();
    client.err_already_exists();
}

#[test]
fn err_unprepared() {
    let mut client = client();
    client.err_unprepared();
}

#[test]
fn snappy() {
    let mut client = snappy_client();
    client.auth();
    client.options();
}

#[test]
fn use_ks() {
    query("USE system");
}

#[test]
fn create_ks() {
    query("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
}

#[ignore]
#[test]
fn drop_ks() {
    query("DROP KEYSPACE test");
}

#[test]
fn create_table() {
    let s = r#"
    CREATE TABLE IF NOT EXISTS test.types (
        uuid uuid,
        custom 'org.apache.cassandra.db.marshal.BooleanType',
        ascii ascii,
        bigint bigint,
        blob blob,
        boolean boolean,
        decimal decimal,
        double double,
        float float,
        int int,
        timestamp timestamp,
        varchar varchar,
        varint varint,
        timeuuid timeuuid,
        inet inet,
        date date,
        time time,
        smallint smallint,
        tinyint tinyint,
        duration duration,
        list list<int>,
        map map<varchar, varchar>,
        hashset set<float>,
        tuple tuple<boolean, double, int, varchar>,
        nested_list list<frozen<list<int>>>,
        nested_set set<frozen<set<int>>>,
        udt phone,
        PRIMARY KEY (uuid)
    )
    "#;
    query(s);
}

#[test]
fn insert() {
    let a = r#"
    INSERT INTO test.types (
        uuid,
        custom,
        ascii,
        bigint,
        blob,
        boolean,
        decimal,
        double,
        float,
        int,
        timestamp,
        varchar,
        varint,
        timeuuid,
        inet,
        date,
        time,
        smallint,
        tinyint,
        duration,
        list,
        map,
        hashset,
        tuple,
        nested_list,
        nested_set,
        udt
    )
    VALUES (
        a2f2466c-9a54-4ca2-ae6b-d346b6962b28,
        true,
        'abcdefg',
        1234567,
        0x012345,
        true,
        123456789.987654321,
        0.123,
        0.456,
        123,
        '2011-02-03 04:05:00.000+0000',
        'abcdefg',
        123456789987654321123456789987654321,
        5943ee37-0000-1000-8000-010203040506,
        '127.0.0.1',
        '1970-01-01',
        '00:00:00',
        0,
        1,
        89h4m48s,
        [1, 2, 3],
        {'a': 'A', 'b': 'B', 'c': 'C'},
        {0.1, 1.2, 2.3, 0.1, 1.2, 2.3},
        (true, 0.1, 1, 'a'),
        [[1, 2, 3], [4, 5, 6]],
        {{0, 1, 2}, {0, 1, 2}, {3, 4, 5}},
        { country_code: 1, number: '202 456-1111' }
    )
    "#;
    let b = r#"
    INSERT INTO test.types (
        uuid,
        custom,
        ascii,
        bigint,
        blob,
        boolean,
        decimal,
        double,
        float,
        int,
        timestamp,
        varchar,
        varint,
        timeuuid,
        inet,
        date,
        time,
        smallint,
        tinyint,
        duration,
        list,
        map,
        hashset,
        tuple,
        nested_list,
        nested_set,
        udt
    )
    VALUES (
        0ffd24bc-a9ae-46b8-803b-1340ff5cbce6,
        false,
        'hijklmn',
        7654321,
        0xabcdef,
        false,
        -123456789.987654321,
        0.456,
        0.789,
        456,
        1299038700000,
        '白兔',
        -123456789987654321,
        5943ee37-0000-1000-8000-010203040506,
        '::1',
        '2019-01-01',
        '23:59:59.123456789',
        2,
        3,
        P0001-02-03T00:00:01,
        [4, 5, 6],
        {'a': 'A', 'b': 'B', 'c': 'C'},
        {0, 1, 2, 2, 1, 0},
        (false, 1.2, 2, 'b'),
        [[7, 8, 9]],
        {{0, 1, 2}, {2, 1, 0}},
        { country_code: 2, number: '13611176765' }
    )
    "#;
    query(a);
    query(b);
}

#[test]
fn create_udt() {
    let s = r#"
    CREATE TYPE IF NOT EXISTS test.phone (
        country_code int,
        number text,
    )
    "#;
    query(s);
}

#[test]
fn select() {
    let a = r#"
    SELECT uuid, custom, ascii, bigint, blob, boolean, decimal, double, float, int, timestamp, varchar, varint,
        timeuuid, inet, date, time, smallint, tinyint, duration, list, map, hashset, tuple, nested_list, nested_set, udt
    FROM test.types
    "#;
    query(a);
}

#[ignore]
#[test]
fn drop_table() {
    query("DROP TABLE test.types");
}

#[test]
fn create_counter_table() {
    let s = r#"
    CREATE TABLE IF NOT EXISTS test.counter (
        uuid uuid,
        counter counter,
        PRIMARY KEY (uuid)
    )
    "#;
    query(s);
}

#[test]
fn insert_counter() {
    let a = r#"
    UPDATE test.counter SET counter = counter + 1
    WHERE uuid = uuid()
    "#;
    query(a);
}

#[test]
fn update_counter() {
    let a = r#"
    UPDATE test.counter SET counter = counter + 1
    WHERE uuid = 4e45acbe-f173-41bd-aa1f-73d84925d7fc
    "#;
    query(a);
}

#[test]
fn select_counter() {
    query("SELECT * FROM test.counter");
}

#[ignore]
#[test]
fn drop_counter_table() {
    query("DROP TABLE test.counter");
}

#[test]
fn create_simple_table() {
    let s = r#"
    CREATE TABLE IF NOT EXISTS test.simple (
        id text,
        name text,
        value int,
        PRIMARY KEY (id)
    )
    "#;
    query(s);
}

#[ignore]
#[test]
fn drop_simple_table() {
    query("DROP TABLE test.simple");
}

#[test]
fn select_simple_table() {
    query("SELECT * FROM test.simple");
}

#[test]
fn trace_select_simple_table() {
    let mut client = client();
    let mut query = Query::from("SELECT * FROM test.simple");
    query.tracing_on();
    let result = client.query(&query);
    let tracing_id = result.tracing_id().unwrap();
    println!("{}", tracing_id);

    let query = Query::from(&format!("SELECT * FROM system_traces.sessions WHERE session_id = {}", tracing_id));
    client.query(&query);
}

#[ignore]
#[test]
fn truncate_simple_table() {
    query("TRUNCATE test.simple");
}

#[test]
fn prepare_insert() {
    let s = "INSERT INTO test.simple (id, name, value) values (?, ?, ?)";
    let prepared = prepare(s);

    let mut params = QueryParams::default();
    let id = Value::Some(marshal_varchar("a").unwrap().unwrap());
    let name = Value::Some(marshal_varchar("aa").unwrap().unwrap());
    let value = Value::Some(marshal_int(&123).unwrap().unwrap());
    params.set_values(vec![id, name, value]);

    let e = Execute::from(prepared.id().to_vec(), params);
    execute(&e);
}

#[test]
fn prepare_named_insert() {
    let s = "INSERT INTO test.simple (id, name, value) values (:i, :n, :v)";
    let prepared = prepare(s);

    let mut params = QueryParams::default();
    params.set_names(vec!["v".to_string(), "i".to_string(), "n".to_string()]);
    let id = Value::Some(marshal_varchar("b").unwrap().unwrap());
    let name = Value::Some(marshal_varchar("bb").unwrap().unwrap());
    let value = Value::Some(marshal_int(&456).unwrap().unwrap());
    params.set_values(vec![value, id, name]);

    let e = Execute::from(prepared.id().to_vec(), params);
    execute(&e);
}

#[test]
fn prepare_select() {
    let s = "SELECT * FROM test.simple where id = ?";
    let prepared = prepare(s);

    let mut params = QueryParams::default();
    let id = Value::Some(marshal_varchar("a").unwrap().unwrap());
    params.set_values(vec![id]);

    let e = Execute::from(prepared.id().to_vec(), params);
    execute(&e);
}

#[test]
fn prepare_named_select() {
    let s = "SELECT * FROM test.simple where id = :id";
    let prepared = prepare(s);

    let mut params = QueryParams::default();
    let id = Value::Some(marshal_varchar("b").unwrap().unwrap());
    params.set_values(vec![id]);

    let e = Execute::from(prepared.id().to_vec(), params);
    execute(&e);
}

#[test]
fn empty_batch() {
    let q = Batch::new();
    batch(&q);
}

#[test]
fn batch_insert() {
    let s = "INSERT INTO test.simple (id, name, value) values ('a', 'aa', 123)";
    let a = BatchQuery::new(BatchQueryKind::Query(LongString::new(s)));

    let s = "INSERT INTO test.simple (id, name, value) values ('b', 'bb', 456)";
    let b = BatchQuery::new(BatchQueryKind::Query(LongString::new(s)));

    let q = Batch::from(BatchType::Logged, vec![a, b]);
    batch(&q);
}

#[test]
fn batch_prepare_insert() {
    let s = "INSERT INTO test.simple (id, name, value) values (?, ?, ?)";
    let prepared = prepare(s);

    let id = Value::Some(marshal_varchar("a").unwrap().unwrap());
    let name = Value::Some(marshal_varchar("aa").unwrap().unwrap());
    let value = Value::Some(marshal_int(&123).unwrap().unwrap());
    let a = BatchQuery::from(BatchQueryKind::Execute(prepared.id().clone()), vec![id, name, value]);

    let id = Value::Some(marshal_varchar("b").unwrap().unwrap());
    let name = Value::Some(marshal_varchar("bb").unwrap().unwrap());
    let value = Value::Some(marshal_int(&456).unwrap().unwrap());
    let b = BatchQuery::from(BatchQueryKind::Execute(prepared.id().clone()), vec![id, name, value]);

    let q = Batch::from(BatchType::Logged, vec![a, b]);
    batch(&q);
}

#[test]
fn create_paging_table() {
    let s = r#"
    CREATE TABLE IF NOT EXISTS test.paging (
        id int,
        value int,
        PRIMARY KEY (id)
    )
    "#;
    query(s);
}

#[ignore]
#[test]
fn truncate_paging_table() {
    query("TRUNCATE test.paging");
}

#[test]
fn insert_paging_table() {
    let s = "INSERT INTO test.paging (id, value) values (?, ?)";
    let prepared = prepare(s);

    let mut querys = Vec::new();
    for i in 1..=100 {
        let id = Value::Some(marshal_int(&i).unwrap().unwrap());
        let value = Value::Some(marshal_int(&i).unwrap().unwrap());
        let query = BatchQuery::from(BatchQueryKind::Execute(prepared.id().clone()), vec![id, value]);
        querys.push(query);
    }

    let b = Batch::from(BatchType::Logged, querys);
    batch(&b);
}

#[test]
fn select_all_paging_table() {
    query("SELECT * FROM test.paging");
}

#[test]
fn select_paging_table() {
    let mut client = client();

    let mut params = QueryParams::default();
    params.set_result_page_size(10);

    let mut query = Query::from("SELECT * FROM test.paging");
    query.set_params(params);
    let result = client.query(&query);

    let rows = if let ResultBody::Rows(rows) = result.body() {
        rows
    } else {
        unreachable!("{:?}", result)
    };
    query.params_mut().set_paging_state(rows.metadata().paging_state().clone());
    client.query(&query);
}

#[ignore]
#[test]
fn drop_paging_table() {
    query("DROP TABLE test.paging");
}

fn query(query: &str) {
    let mut client = client();
    let query = Query::from(query);
    client.query(&query);
}

fn prepare(query: &str) -> Prepared {
    let mut client = client();
    let prepare = Prepare::from(query);
    client.prepare(&prepare)
}

fn execute(execute: &Execute) {
    let mut client = client();
    client.execute(execute);
}

fn batch(batch: &Batch) {
    let mut client = client();
    client.batch(batch);
}

