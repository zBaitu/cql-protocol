# cql-protocol

https://github.com/zBaitu/cql-protocol


# Overview
This is a [CQL (Cassandra Query Language)](https://cassandra.apache.org/doc/latest/cql/index.html) spec implementation written in Rust.
It is not a completed Cassandra client, it likes the [Datastax native-protocol](https://github.com/datastax/native-protocol) that just implements the [CQL specification](https://github.com/datastax/native-protocol/tree/1.x/src/main/resources).
Of course it can be used as a basic lib for writing a real Cassandra client for Rust.


# Code Structure
```
.
├── codec.rs
├── compression.rs
├── def.rs
├── frame.rs
├── lib.rs
├── message.rs
├── request
│   ├── auth_response.rs
│   ├── batch.rs
│   ├── execute.rs
│   ├── mod.rs
│   ├── options.rs
│   ├── prepare.rs
│   ├── query.rs
│   ├── register.rs
│   └── startup.rs
├── response
│   ├── auth_challenge.rs
│   ├── auth_success.rs
│   ├── authenticate.rs
│   ├── error.rs
│   ├── event.rs
│   ├── mod.rs
│   ├── ready.rs
│   ├── result.rs
│   └── supported.rs
├── result.rs
├── types.rs
└── vint.rs
```

- codec: Serde for body in frame, corespoding to the [Notations](https://github.com/datastax/native-protocol/blob/1.x/src/main/resources/native_protocol_v5.spec) part in spec.
- compression: Compression trait for lz4 and snappy
- def: Constants and definitions.
- frame: The Frame header part of spec.
- message: Message trait for request and response message.
- types: Mapping between Rust and CQL types.
- vint: Variable Length Integer.
- request, response: Every request and response message implementation.


# Try It
`tests/client.rs` contains every message usage. Support for DDL and DML, support for query, prepare, execute, batch operations. For example:
```
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

#[test]
fn create_ks() {
    query("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
}
```
