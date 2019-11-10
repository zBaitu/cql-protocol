use crate::codec::*;
use crate::def::*;
use crate::message::*;
use crate::response::event::SchemaChange;
use crate::result::*;
use crate::types::*;

use num_traits::FromPrimitive;

use std::{
    fmt::{self, Debug, Display, Formatter},
    io
};

const EMPTY_COL_SPECS: Vec<ColSpec> = Vec::new();

#[derive(Debug, Default, PartialEq)]
pub struct Void {}

impl Serializable for Void {
    fn length(&self) -> u32 { 0 }

    fn encode<B: io::Read + io::Write>(&self, _codec: &mut Codec<B>) -> ProtResult<()> { OK }

    fn decode<B: io::Read + io::Write>(_codec: &mut Codec<B>) -> ProtResult<Self> where Self: Sized {
        Ok(Void {})
    }
}

#[derive(Debug, PartialEq)]
pub struct GlobalTableSpec {
    pub keyspace: String,
    pub table: String,
}

impl GlobalTableSpec {
    pub fn new(ks: &str, table: &str) -> Self {
        GlobalTableSpec {
            keyspace: ks.to_string(),
            table: table.to_string(),
        }
    }
}

impl Serializable for GlobalTableSpec {
    fn length(&self) -> u32 {
        self.keyspace.length() + self.table.length()
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        codec.write_string(&self.keyspace)?;
        codec.write_string(&self.table)?;
        OK
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> where Self: Sized {
        Ok(GlobalTableSpec::new(&codec.read_string()?, &codec.read_string()?))
    }
}

#[derive(Debug, PartialEq)]
pub struct ColSpec {
    ks_table: Option<GlobalTableSpec>,
    name: String,
    ty: Opt,
}

impl ColSpec {
    pub fn new(name: &str, ty: Opt) -> Self {
        ColSpec {
            ks_table: None,
            name: name.to_string(),
            ty,
        }
    }

    pub fn set_global_table_spec(&mut self, global_table_spec: GlobalTableSpec) {
        self.ks_table = Some(global_table_spec);
    }

    pub fn global_table_spec(&self) -> &Option<GlobalTableSpec> {
        &self.ks_table
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn ty(&self) -> &Opt {
        &self.ty
    }

    pub fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>, is_global_table_spec: bool) -> ProtResult<Self>
    where Self: Sized {
        let ks_table = if is_global_table_spec {
            None
        } else {
            Some(GlobalTableSpec::decode(codec)?)
        };

        Ok(ColSpec {
            ks_table,
            name: codec.read_string()?,
            ty: codec.read_option()?,
        })
    }
}

impl Serializable for ColSpec {
    fn length(&self) -> u32 {
        let mut len = 0;
        if let Some(ref ks_table) = self.ks_table {
            len += ks_table.length();
        }
        len + self.name.length() + self.ty.length()
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        if let Some(ref ks_table) = self.ks_table {
            ks_table.encode(codec)?;
        }
        codec.write_string(&self.name)?;
        codec.write_option(&self.ty)?;
        OK
    }

    fn decode<B: io::Read + io::Write>(_codec: &mut Codec<B>) -> ProtResult<Self> where Self: Sized {
        unimplemented!()
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct RowsMetadata {
    no_metadata: bool,
    paging_state: Bytes,
    new_metadata_id: Option<ShortBytes>,
    ks_table: Option<GlobalTableSpec>,
    col_specs: Vec<ColSpec>,
}

impl RowsMetadata {
    pub fn set_no_metadata(&mut self) {
        self.no_metadata = true;
    }

    pub fn set_paging_state(&mut self, paging_state: Bytes) {
        self.paging_state = paging_state;
    }

    pub fn set_new_metadata_id(&mut self, new_metadata_id: ShortBytes) {
        self.new_metadata_id = Some(new_metadata_id);
    }

    pub fn set_global_table_spec(&mut self, global_table_spec: GlobalTableSpec) {
        self.ks_table = Some(global_table_spec);
    }

    pub fn set_col_specs(&mut self, col_spec: Vec<ColSpec>) {
        self.col_specs = col_spec;
    }

    pub fn paging_state(&self) -> &Bytes {
        &self.paging_state
    }

    pub fn new_metadata_id(&self) -> &Option<ShortBytes> {
        &self.new_metadata_id
    }

    pub fn global_table_spec(&self) -> &Option<GlobalTableSpec> {
        &self.ks_table
    }

    pub fn col_specs(&self) -> &Vec<ColSpec> {
        &self.col_specs
    }

    pub fn col_type(&self, i: usize) -> &Opt {
        &self.col_specs[i].ty
    }

    fn flags(&self) -> Int {
        let mut flags = 0;
        if self.ks_table.is_some() {
            flags |= RowsFlags::GlobalTablesSpec;
        }
        if self.paging_state.is_some() {
            flags |= RowsFlags::HasMorePages;
        }
        if self.no_metadata {
            flags |= RowsFlags::NoMetadata;
        }
        if self.new_metadata_id.is_some() {
            flags |= RowsFlags::MetadataChanged;
        }
        flags
    }
}

impl Serializable for RowsMetadata {
    fn length(&self) -> u32 {
        let mut len = len::INT * 2 + self.paging_state.length();
        if let Some(ref new_metadata_id) = self.new_metadata_id {
            len += new_metadata_id.length();
        }
        if let Some(ref ks_table) = self.ks_table {
            len += ks_table.length();
        }
        self.col_specs.iter().fold(len, |len, e| len + e.length());
        len
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        codec.write_int(self.flags())?;
        codec.write_int(self.col_specs.len() as Int)?;
        codec.write_bytes(&self.paging_state)?;
        if let Some(ref new_metadata_id) = self.new_metadata_id {
            codec.write_short_bytes(&new_metadata_id)?;
        }

        if self.no_metadata {
            return OK
        }

        if let Some(ref ks_table) = self.ks_table {
            ks_table.encode(codec)?;
        }
        for col_spec in &self.col_specs {
            col_spec.encode(codec)?;
        }
        OK
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> where Self: Sized {
        let flags = codec.read_int()?;
        let col_specs_len = codec.read_int()?;

        let paging_state = if RowsFlags::HasMorePages.is_set(flags) {
            codec.read_bytes()?
        } else {
            None
        };
        let new_metadata_id = if RowsFlags::MetadataChanged.is_set(flags) {
            Some(codec.read_short_bytes()?)
        } else {
            None
        };

        if RowsFlags::NoMetadata.is_set(flags) {
            return Ok(RowsMetadata {
                no_metadata: true,
                paging_state,
                new_metadata_id,
                ks_table: None,
                col_specs: EMPTY_COL_SPECS,
            });
        }

        let is_global_table_spec = RowsFlags::GlobalTablesSpec.is_set(flags);
        let ks_table = if is_global_table_spec {
            Some(GlobalTableSpec::decode(codec)?)
        } else {
            None
        };
        let mut col_specs = Vec::new();
        for _ in 1..=col_specs_len {
            col_specs.push(ColSpec::decode(codec, is_global_table_spec)?);
        }
        Ok(RowsMetadata {
            no_metadata: false,
            paging_state,
            new_metadata_id,
            ks_table,
            col_specs,
        })
    }
}

#[derive(Default, PartialEq)]
pub struct Rows {
    metadata: RowsMetadata,
    content: Vec<Vec<Bytes>>,
}

impl Rows {
    pub fn new(metadata: RowsMetadata, content: Vec<Vec<Bytes>>) -> Self {
        Rows {
            metadata,
            content,
        }
    }

    pub fn metadata(&self) -> &RowsMetadata {
        &self.metadata
    }

    fn content_length(&self) -> u32 {
        self.content.iter().fold(0, |len, e| len + e.iter().fold(0, |len, e| len + e.length()))
    }

    fn encode_content<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        for row in &self.content {
            for col in row {
                codec.write_bytes(col)?;
            }
        }
        OK
    }

    fn decode_content<B: io::Read + io::Write>(codec: &mut Codec<B>, row_len: Int, col_len: Int)
    -> ProtResult<Vec<Vec<Bytes>>> {
        let mut content = Vec::new();
        for _ in 1..=row_len {
            let mut row = Vec::new();
            for _ in 1..=col_len {
                row.push(codec.read_bytes()?);
            }
            content.push(row);
        }
        Ok(content)
    }
}

impl Serializable for Rows {
    fn length(&self) -> u32 {
        self.metadata.length() + self.content_length()
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        self.metadata.encode(codec)?;
        self.encode_content(codec)?;
        OK
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> where Self: Sized {
        let metadata = RowsMetadata::decode(codec)?;
        let row_len = codec.read_int()?;
        let content = Rows::decode_content(codec, row_len, metadata.col_specs().len() as Int)?;
        Ok(Rows {
            metadata,
            content,
        })
    }
}

impl Debug for Rows {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        Debug::fmt(&self.metadata, f)?;
        write!(f, "\n")?;

        for row in &self.content {
            write!(f, "[")?;
            let mut first = true;
            for (i, col) in row.iter().enumerate() {
                if first {
                    first = false;
                } else {
                    write!(f, ", ")?;
                }

                if col.is_none() {
                    write!(f, "null")?;
                    continue;
                }

                let col_type = self.metadata.col_type(i);
                let v = unmarshal(col_type, col.as_ref().unwrap()).unwrap();
                write!(f, "{:?}", v)?;
            }
            write!(f, "]\n")?;
        }
        write!(f, "{}\n", self.content.len())?;
        Ok(())
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct SetKeyspace {
    ks: String,
}

impl SetKeyspace {
    pub fn new(ks: &str) -> Self {
        SetKeyspace {
            ks: ks.to_string(),
        }
    }
}

impl Serializable for SetKeyspace {
    fn length(&self) -> u32 {
        self.ks.length()
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        codec.write_string(&self.ks)
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> where Self: Sized {
        Ok(SetKeyspace {
            ks: codec.read_string()?,
        })
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct PreparedMetadata {
    pk_indices: Vec<Short>,
    ks_table: Option<GlobalTableSpec>,
    col_specs: Vec<ColSpec>,
}

impl PreparedMetadata {
    pub fn set_pk_indices(&mut self, pk_indices: Vec<Short>) {
        self.pk_indices = pk_indices;
    }

    pub fn set_global_table_spec(&mut self, global_table_spec: GlobalTableSpec) {
        self.ks_table = Some(global_table_spec);
    }

    pub fn set_col_specs(&mut self, col_spec: Vec<ColSpec>) {
        self.col_specs = col_spec;
    }

    pub fn pk_indices(&self) -> &Vec<Short> {
        &self.pk_indices
    }

    pub fn global_table_spec(&self) -> &Option<GlobalTableSpec> {
        &self.ks_table
    }

    pub fn col_specs(&self) -> &Vec<ColSpec> {
        &self.col_specs
    }

    fn flags(&self) -> Int {
        let mut flags = 0;
        if self.ks_table.is_some() {
            flags |= PreparedFlags::GlobalTablesSpec;
        }
        flags
    }
}

impl Serializable for PreparedMetadata {
    fn length(&self) -> u32 {
        let mut len = len::INT * 3 + len::SHORT * self.pk_indices.len() as u32;
        if let Some(ref ks_table) = self.ks_table {
            len += ks_table.length();
        }
        self.col_specs.iter().fold(len, |len, e| len + e.length());
        len
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        codec.write_int(self.flags())?;
        codec.write_int(self.col_specs.len() as Int)?;
        codec.write_int(self.pk_indices.len() as Int)?;

        for pk_index in &self.pk_indices {
            codec.write_short(*pk_index)?;
        }
        if let Some(ref ks_table) = self.ks_table {
            ks_table.encode(codec)?;
        }
        for col_spec in &self.col_specs {
            col_spec.encode(codec)?;
        }
        OK
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> where Self: Sized {
        let flags = codec.read_int()?;
        let col_specs_len = codec.read_int()?;
        let pk_indices_len = codec.read_int()?;

        let mut pk_indices = Vec::new();
        for _ in 1..=pk_indices_len {
            pk_indices.push(codec.read_short()?);
        }

        let is_global_table_spec = PreparedFlags::GlobalTablesSpec.is_set(flags);
        let ks_table = if is_global_table_spec {
            Some(GlobalTableSpec::decode(codec)?)
        } else {
            None
        };

        let mut col_specs = Vec::new();
        for _ in 1..=col_specs_len {
            col_specs.push(ColSpec::decode(codec, is_global_table_spec)?);
        }

        Ok(PreparedMetadata {
            pk_indices,
            ks_table,
            col_specs,
        })
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct Prepared {
    id: ShortBytes,
    metadata: PreparedMetadata,
    result_metadata: RowsMetadata,
}

impl Prepared {
    pub fn new(id: ShortBytes, metadata: PreparedMetadata, result_metadata: RowsMetadata) -> Self {
        Prepared {
            id,
            metadata,
            result_metadata,
        }
    }

    pub fn id(&self) -> &ShortBytes {
        &self.id
    }

    pub fn metadata(&self) -> &PreparedMetadata {
        &self.metadata
    }

    pub fn result_metadata(&self) -> &RowsMetadata {
        &self.result_metadata
    }
}

impl Serializable for Prepared {
    fn length(&self) -> u32 {
        self.id.length() + self.metadata.length() + self.result_metadata.length()
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        codec.write_short_bytes(&self.id)?;
        self.metadata.encode(codec)?;
        self.result_metadata.encode(codec)?;
        OK
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> where Self: Sized {
        let id = codec.read_short_bytes()?;
        let metadata = PreparedMetadata::decode(codec)?;
        let result_metadata = RowsMetadata::decode(codec)?;

        Ok(Prepared {
            id,
            metadata,
            result_metadata,
        })
    }
}

impl_display!(Void);
impl_display!(GlobalTableSpec);
impl_display!(ColSpec);
impl_display!(RowsMetadata);
impl_display!(Rows);
impl_display!(SetKeyspace);
impl_display!(PreparedMetadata);
impl_display!(Prepared);

macro_rules! match_result_kind {
    ($sf:expr, $($R:ident),+) => (
        match $sf {
            $(ResultBody::$R(_) => ResultKind::$R,)*
        }
    );
}

macro_rules! match_length {
    ($sf:expr, $($R:ident),+) => (
        match $sf {
            $(ResultBody::$R(ref r) => r.length(),)*
        }
    );
}

macro_rules! match_encode {
    ($sf:expr, $codec:expr, $($R:ident),+) => (
        match $sf {
            $(ResultBody::$R(ref r) => r.encode($codec),)*
        }
    );
}

macro_rules! match_decode {
    ($event_type:expr, $codec:expr, $($R:ident),+) => (
        match $event_type {
            $(ResultKind::$R => ResultBody::$R($R::decode($codec)?),)*
        }
    );
}

#[derive(Debug, PartialEq)]
pub enum ResultBody {
    Void(Void),
    Rows(Rows),
    SetKeyspace(SetKeyspace),
    Prepared(Prepared),
    SchemaChange(SchemaChange),
}

#[derive(Debug, PartialEq)]
pub struct Result {
    body: ResultBody,
    tracing_id: Option<Uuid>,
}

impl Result {
    pub fn new(body: ResultBody) -> Self {
        Result {
            body,
            tracing_id: None,
        }
    }

    pub fn set_tracing_id(&mut self, tracing_id: Option<Uuid>) {
        self.tracing_id = tracing_id;
    }

    pub fn body(&self) -> &ResultBody {
        &self.body
    }

    pub fn into_body(self) -> ResultBody {
        self.body
    }

    pub fn tracing_id(&self) -> &Option<Uuid> {
        &self.tracing_id
    }
}

impl Message for Result {
    fn opcode(&self) -> Opcode { Opcode::Result }
}

impl Serializable for Result {
    fn length(&self) -> u32 {
        len::INT + match_length!(self.body, Void, Rows, SetKeyspace, Prepared, SchemaChange)
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        let result_kind = match_result_kind!(self.body, Void, Rows, SetKeyspace, Prepared, SchemaChange);
        codec.write_int(result_kind as Int)?;
        match_encode!(self.body, codec, Void, Rows, SetKeyspace, Prepared, SchemaChange)
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> {
        let result_kind = FromPrimitive::from_i32(codec.read_int()?).unwrap();
        let result = match_decode!(result_kind, codec, Void, Rows, SetKeyspace, Prepared, SchemaChange);
        Ok(Result::new(result))
    }
}
