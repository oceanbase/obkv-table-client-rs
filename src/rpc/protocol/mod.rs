/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2021 OceanBase
 * %%
 * OBKV Table Client Framework is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the
 * Mulan PSL v2. You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * #L%
 */

use std::{
    fmt,
    io::{self, Cursor},
    net::SocketAddr,
    sync::atomic::{AtomicI32, Ordering},
};

use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::{error::Error, serde_obkv::util, util as u};

pub mod codes;
pub mod payloads;
pub mod query;

pub mod partition;

pub type Result<T> = std::result::Result<T, std::io::Error>;

pub trait ProtoEncoder {
    fn encode(&self, dst: &mut BytesMut) -> Result<()>;
}

pub trait ProtoDecoder {
    fn decode(&mut self, src: &mut BytesMut) -> Result<()>;
}

// compression type for packet
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObCompressType {
    Invalid = 0,
    None = 1,
    LZ4 = 2,
    Snappy = 3,
    Zlib = 4,
    Zstd = 5,
}

impl ObCompressType {
    pub fn from_i32(i: i32) -> Result<ObCompressType> {
        match i {
            0 => Ok(ObCompressType::Invalid),
            1 => Ok(ObCompressType::None),
            2 => Ok(ObCompressType::LZ4),
            3 => Ok(ObCompressType::Snappy),
            4 => Ok(ObCompressType::Zlib),
            5 => Ok(ObCompressType::Zstd),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("ObCompressType::from_i32 invalid compress type, i={i}"),
            )),
        }
    }
}

pub const PCODE_LOGIN: u16 = 0x1101;
pub const PCODE_EXECUTE: u16 = 0x1102;
pub const PCODE_BATCH_EXECUTE: u16 = 0x1103;
pub const PCODE_EXECUTE_QUERY: u16 = 0x1104;
pub const PCODE_QUERY_AND_MUTE: u16 = 0x1105;
pub const PCODE_ERROR_PACKET: u16 = 0x010;

#[derive(Clone, Copy, Debug)]
pub struct TraceId(pub u64, pub u64);

impl fmt::Display for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Y{:X}-{:016X}", self.0, self.1)
    }
}

/// ObTable packet command code
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObTablePacketCode {
    Login,
    Execute,
    BatchExecute,
    ExecuteQuery,
    QueryAndMute,
    Error,
}

impl ObTablePacketCode {
    pub fn value(&self) -> u16 {
        match *self {
            ObTablePacketCode::Login => PCODE_LOGIN,
            ObTablePacketCode::Execute => PCODE_EXECUTE,
            ObTablePacketCode::BatchExecute => PCODE_BATCH_EXECUTE,
            ObTablePacketCode::ExecuteQuery => PCODE_EXECUTE_QUERY,
            ObTablePacketCode::QueryAndMute => PCODE_QUERY_AND_MUTE,
            ObTablePacketCode::Error => PCODE_ERROR_PACKET,
        }
    }

    pub fn from_u16(i: u16) -> Result<ObTablePacketCode> {
        match i {
            PCODE_LOGIN => Ok(ObTablePacketCode::Login),
            PCODE_EXECUTE => Ok(ObTablePacketCode::Execute),
            PCODE_BATCH_EXECUTE => Ok(ObTablePacketCode::BatchExecute),
            PCODE_EXECUTE_QUERY => Ok(ObTablePacketCode::ExecuteQuery),
            PCODE_QUERY_AND_MUTE => Ok(ObTablePacketCode::QueryAndMute),
            PCODE_ERROR_PACKET => Ok(ObTablePacketCode::Error),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("ObTablePacketCode::from_u16 invalid value, i={i}"),
            )),
        }
    }
}

pub const HEADER_SIZE: usize = 72;
pub const COST_TIME_ENCODE_SIZE: usize = 40;
/// RPC request default timeout in milliseconds.
pub const OP_TIMEOUT: i64 = 10000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObRpcCostTime {
    len: i32,
    arrival_push_diff: i32,
    push_pop_diff: i32,
    pop_process_start_diff: i32,
    process_start_end_diff: i32,
    process_end_response_diff: i32,
    packet_id: i64,
    request_arrive_time: i64,
}

impl Default for ObRpcCostTime {
    fn default() -> ObRpcCostTime {
        ObRpcCostTime::new()
    }
}

impl ObRpcCostTime {
    pub fn new() -> ObRpcCostTime {
        ObRpcCostTime {
            len: COST_TIME_ENCODE_SIZE as i32,
            arrival_push_diff: 0,
            push_pop_diff: 0,
            pop_process_start_diff: 0,
            process_start_end_diff: 0,
            process_end_response_diff: 0,
            packet_id: 0,
            request_arrive_time: 0,
        }
    }
}

impl ProtoEncoder for ObRpcCostTime {
    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        buf.reserve(COST_TIME_ENCODE_SIZE);

        buf.put_i32(self.len);
        buf.put_i32(self.arrival_push_diff);
        buf.put_i32(self.push_pop_diff);
        buf.put_i32(self.pop_process_start_diff);
        buf.put_i32(self.process_start_end_diff);
        buf.put_i32(self.process_end_response_diff);
        buf.put_i64(self.packet_id);
        buf.put_i64(self.request_arrive_time);

        Ok(())
    }
}

impl ProtoDecoder for ObRpcCostTime {
    fn decode(&mut self, buf: &mut BytesMut) -> Result<()> {
        if buf.len() < COST_TIME_ENCODE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "ObRpcCostTime::decode buffer not enough data: expects {}, but has {}",
                    COST_TIME_ENCODE_SIZE,
                    buf.len()
                ),
            ));
        }

        {
            let mut buf = Cursor::new(&mut *buf);
            self.len = buf.get_i32();
            self.arrival_push_diff = buf.get_i32();
            self.push_pop_diff = buf.get_i32();
            self.pop_process_start_diff = buf.get_i32();
            self.process_start_end_diff = buf.get_i32();
            self.process_end_response_diff = buf.get_i32();
            self.packet_id = buf.get_i64();
            self.request_arrive_time = buf.get_i64();
        }
        buf.advance(COST_TIME_ENCODE_SIZE);
        Ok(())
    }
}

pub const RPC_PACKET_HEADER_SIZE: usize = HEADER_SIZE + COST_TIME_ENCODE_SIZE //
    + 8 // cluster id
    + 4 // compress type
    + 4 // original len
;
// version:V4
pub const RPC_PACKET_HEADER_SIZE_V4: usize = RPC_PACKET_HEADER_SIZE
    + 8 // src clusterId
    + 8 // unis version
    + 4 // request level
    + 8 // seq no
    + 4 // group id
    + 8 // trace id2
    + 8 // trace id3
    + 8 //clusterNameHash
;

/*
 *
 * pcode              (4  bytes) {@code ObTablePacketCode}
 * hlen               (1  byte)  unsigned byte
 * priority           (1  byte)  unsigned byte
 * flag               (2  bytes) unsigned short
 * checksum           (8  bytes) long
 * tenantId           (8  bytes) unsigned long
 * prvTenantId        (8  bytes) unsigned long
 * sessionId          (8  bytes) unsigned long
 * traceId0           (8  bytes) unsigned long
 * traceId1           (8  bytes) unsigned long
 * timeout            (8  bytes) unsigned long
 * timestamp          (8  bytes) long
 * rpc_cost_time      (40 bytes) {@code ObRpcCostTime}
 * cluster_id         (8  bytes) long
 * compress_type      (4  bytes) long
 * original_len       (4  bytes) long
 * src_cluster_id     (8  bytes) long
 * unis_version       (8  bytes) long
 * request_level      (4  bytes) int
 * seq_no             (8  bytes) long
 * group_id           (4  bytes) long
 * trace_id2          (8  bytes) long
 * trace_id3          (8  bytes) long
 * cluster_name_hash  (8  bytes) long
 */
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObRpcPacketHeader {
    pcode: u32,
    hlen: u8,
    priority: u8,
    flag: u16,
    checksum: i64,
    tenant_id: u64,
    prev_tenant_id: u64,
    session_id: u64,
    trace_id0: u64,
    trace_id1: u64,
    timeout: i64,
    timestamp: i64,
    rpc_cost_time: ObRpcCostTime,
    cluster_id: i64,
    // dst cluster id
    compress_type: ObCompressType,
    // original length before compression.
    original_len: i32,
    // new packet header for version: V4
    src_cluster_id: i64,
    unis_version: i64,
    request_level: i32,
    seq_no: i64,
    group_id: i32,
    trace_id2: i64,
    trace_id3: i64,
    cluster_name_hash: i64,
}

impl Default for ObRpcPacketHeader {
    fn default() -> ObRpcPacketHeader {
        ObRpcPacketHeader::new()
    }
}

const STREAM_FLAG: u16 = 1 << 14;
const RESP_FLAG: u16 = 1 << 15;
const STREAM_LAST_FLAG: u16 = 1 << 13;
// OB_LOG_LEVEL_NONE 7
// OB_LOG_LEVEL_NP -1 (no print log)
// OB_LOG_LEVEL_ERROR 0
// OB_LOG_LEVEL_WARN 2
// OB_LOG_LEVEL_INFO 3
// OB_LOG_LEVEL_TRACE 4
// OB_LOG_LEVEL_DEBUG 5
pub const DEFAULT_FLAG: u16 = 7;

impl ObRpcPacketHeader {
    pub fn new() -> ObRpcPacketHeader {
        ObRpcPacketHeader {
            pcode: 0_u32,
            hlen: RPC_PACKET_HEADER_SIZE_V4 as u8,
            priority: 5,
            flag: DEFAULT_FLAG,
            checksum: 0,
            tenant_id: 1,
            prev_tenant_id: 1,
            session_id: 0,
            trace_id0: 0,
            trace_id1: 0,
            timeout: OP_TIMEOUT * 1000, // OB server timeout(us)
            timestamp: u::current_time_millis() * 1000, //(us)
            rpc_cost_time: ObRpcCostTime::new(),
            cluster_id: -1,
            compress_type: ObCompressType::Invalid, //i32
            original_len: 0,                        //original length before compression.
            //new packet for version: V4
            src_cluster_id: -1,
            unis_version: 0,
            request_level: 0,
            seq_no: 0,
            group_id: 0,
            trace_id2: 0,
            trace_id3: 0,
            cluster_name_hash: 0,
        }
    }

    #[inline]
    pub fn set_pcode(&mut self, c: u32) {
        self.pcode = c;
    }

    #[inline]
    pub fn set_trace_id(&mut self, id: TraceId) {
        self.trace_id0 = id.0;
        self.trace_id1 = id.1;
    }

    #[inline]
    pub fn is_empty_trace_id(&self) -> bool {
        self.trace_id0 == 0 && self.trace_id1 == 0
    }

    #[inline]
    pub fn trace_id(&self) -> TraceId {
        TraceId(self.trace_id0, self.trace_id1)
    }

    #[inline]
    pub fn timeout(&self) -> i64 {
        self.timeout
    }

    #[inline]
    pub fn set_timeout(&mut self, timeout: i64) {
        self.timeout = timeout
    }

    #[inline]
    pub fn set_checksum(&mut self, checksum: i64) {
        self.checksum = checksum;
    }

    #[inline]
    pub fn set_tenant_id(&mut self, tenant_id: u64) {
        self.tenant_id = tenant_id;
    }

    #[inline]
    pub fn set_session_id(&mut self, session_id: u64) {
        self.session_id = session_id;
    }

    #[inline]
    pub fn set_flag(&mut self, flag: u16) {
        self.flag = flag;
    }

    #[inline]
    pub fn is_stream(&self) -> bool {
        self.flag & STREAM_FLAG != 0
    }

    #[inline]
    pub fn is_stream_next(&self) -> bool {
        self.is_stream() && self.flag & STREAM_LAST_FLAG == 0
    }

    #[inline]
    pub fn is_stream_last(&self) -> bool {
        self.is_stream() && self.flag & STREAM_LAST_FLAG != 0
    }

    #[inline]
    pub fn is_response(&self) -> bool {
        self.flag & RESP_FLAG != 0
    }

    #[inline]
    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    #[inline]
    fn get_encoded_size_with_cost_time(&self) -> usize {
        HEADER_SIZE + COST_TIME_ENCODE_SIZE
    }

    #[inline]
    fn get_encoded_size_with_cost_time_and_cluster_id(&self) -> usize {
        self.get_encoded_size_with_cost_time() + 8 // cluster id
    }

    #[inline]
    fn get_encoded_size(&self) -> usize {
        self.get_encoded_size_with_cost_time_and_cluster_id()
            + 4 // ob_compression_type
            + 4 // original_len
    }

    #[inline]
    fn get_encoded_size_with_newserver(&self) -> usize {
        RPC_PACKET_HEADER_SIZE_V4
    }
}

/// Rpc packet
pub struct ObRpcPacket {
    header: ObRpcPacketHeader,
    payload: BytesMut,
}

impl ObRpcPacket {
    pub fn new(header: ObRpcPacketHeader, payload: BytesMut) -> ObRpcPacket {
        ObRpcPacket { header, payload }
    }
}

impl ProtoEncoder for ObRpcPacket {
    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        self.header.encode(buf)?;
        buf.reserve(self.payload.len());
        buf.put_slice(&self.payload);
        Ok(())
    }
}

impl ProtoEncoder for ObRpcPacketHeader {
    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        buf.reserve(RPC_PACKET_HEADER_SIZE_V4);
        buf.put_u32(self.pcode);
        buf.put_u8(self.hlen);
        buf.put_u8(self.priority);
        buf.put_u16(self.flag);
        buf.put_i64(self.checksum);
        buf.put_u64(self.tenant_id);
        buf.put_u64(self.prev_tenant_id);
        buf.put_u64(self.session_id);
        buf.put_u64(self.trace_id0);
        buf.put_u64(self.trace_id1);
        buf.put_i64(self.timeout);
        buf.put_i64(self.timestamp);
        self.rpc_cost_time.encode(buf)?;
        buf.put_i64(self.cluster_id);
        buf.put_i32(self.compress_type.clone() as i32);
        buf.put_i32(self.original_len);
        // TODO: check observer 4.x is ok
        buf.put_i64(self.src_cluster_id);
        buf.put_i64(self.unis_version);
        buf.put_i32(self.request_level);
        buf.put_i64(self.seq_no);
        buf.put_i32(self.group_id);
        buf.put_i64(self.trace_id2);
        buf.put_i64(self.trace_id3);
        buf.put_i64(self.cluster_name_hash);

        Ok(())
    }
}

impl ProtoDecoder for ObRpcPacketHeader {
    fn decode(&mut self, buf: &mut BytesMut) -> Result<()> {
        let mut src = util::split_buf_to(buf, HEADER_SIZE).map_err(|e| {
            error!(
                "ObRpcPacketHeader::decode fail to split basic header, err:{}",
                e
            );
            e
        })?;

        self.pcode = src.get_u32();
        self.hlen = src.get_u8();
        self.priority = src.get_u8();
        self.flag = src.get_u16();
        self.checksum = src.get_i64();
        self.tenant_id = src.get_u64();
        self.prev_tenant_id = src.get_u64();
        self.session_id = src.get_u64();
        self.trace_id0 = src.get_u64();
        self.trace_id1 = src.get_u64();
        self.timeout = src.get_i64();
        self.timestamp = src.get_i64();

        let hlen = self.hlen as usize;

        let ignore_len = if hlen >= self.get_encoded_size_with_newserver() {
            // decode header for version: V4
            // header with cost_time, cluster_id, compress_type, original_len,
            // src_cluster_id(i64), unis_version(i64), request_level(i32),
            // seq_no(i64), group_id(i32), trace_id2(i64), trace_id3(i64),
            // cluster_name_hash(i64)
            self.rpc_cost_time.decode(buf)?;
            let mut src = util::split_buf_to(
                buf,
                self.get_encoded_size_with_newserver() - self.get_encoded_size_with_cost_time(),
            )?;
            self.cluster_id = src.get_i64();
            self.compress_type = ObCompressType::from_i32(src.get_i32())?;
            self.original_len = src.get_i32();
            //  decode for version:V4
            self.src_cluster_id = src.get_i64();
            self.unis_version = src.get_i64();
            self.request_level = src.get_i32();
            self.seq_no = src.get_i64();
            self.group_id = src.get_i32();
            self.trace_id2 = src.get_i64();
            self.trace_id3 = src.get_i64();
            self.cluster_name_hash = src.get_i64();
            hlen - self.get_encoded_size_with_newserver()
        } else if hlen >= self.get_encoded_size() {
            // header with cost_time, cluster_id, compress_type, original_len
            self.rpc_cost_time.decode(buf)?;
            let mut src = util::split_buf_to(
                buf,
                self.get_encoded_size() - self.get_encoded_size_with_cost_time(),
            )?;
            self.cluster_id = src.get_i64();
            self.compress_type = ObCompressType::from_i32(src.get_i32())?;
            self.original_len = src.get_i32();
            hlen - self.get_encoded_size()
        } else if hlen >= self.get_encoded_size_with_cost_time_and_cluster_id() {
            // header with cost_time, cluster_id
            self.rpc_cost_time.decode(buf)?;
            let mut src = util::split_buf_to(
                buf,
                self.get_encoded_size_with_cost_time_and_cluster_id()
                    - self.get_encoded_size_with_cost_time(),
            )?;
            self.cluster_id = src.get_i64();
            hlen - self.get_encoded_size_with_cost_time_and_cluster_id()
        } else if hlen >= self.get_encoded_size_with_cost_time() {
            self.rpc_cost_time.decode(buf)?;
            hlen - self.get_encoded_size_with_cost_time()
        } else if hlen >= HEADER_SIZE {
            hlen - HEADER_SIZE
        } else {
            error!(
                "ObRpcPacketHeader::decode error, hlen={}, expect at least:{}.",
                hlen, HEADER_SIZE
            );
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid RPC header {self:?}."),
            ));
        };

        // ignore the useless bytes
        buf.advance(ignore_len);

        Ok(())
    }
}

const VERSION: i64 = 1;

static CHANNEL_ID: AtomicI32 = AtomicI32::new(0);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasePayLoad {
    channel_id: i32,
    version: i64,
    timeout: i64,
    flag: u16,
}

/// Base payload for all payloads
impl BasePayLoad {
    pub fn new() -> Self {
        BasePayLoad {
            channel_id: CHANNEL_ID.fetch_add(1, Ordering::Relaxed),
            version: VERSION,
            timeout: OP_TIMEOUT,
            flag: DEFAULT_FLAG,
        }
    }

    pub fn dummy() -> Self {
        Self::new()
    }
}

impl Default for BasePayLoad {
    fn default() -> Self {
        BasePayLoad::new()
    }
}

// Payload trait
pub trait ObPayload: ProtoEncoder + ProtoDecoder {
    fn channel_id(&self) -> i32 {
        self.base().channel_id
    }
    fn pcode(&self) -> ObTablePacketCode {
        ObTablePacketCode::Error
    }
    fn timeout_millis(&self) -> i64 {
        self.base().timeout
    }
    fn len(&self) -> Result<usize> {
        let clen = self.content_len()?;
        Ok(util::encoded_length_vi64(VERSION) + util::encoded_length_vi64(clen as i64) + clen)
    }
    fn base(&self) -> &BasePayLoad;
    fn base_mut(&mut self) -> &mut BasePayLoad;
    //payload size, without header bytes
    fn content_len(&self) -> Result<usize> {
        Ok(0)
    }

    fn encode_header(&self, buf: &mut BytesMut) -> Result<()> {
        let len = self.len()?;
        buf.reserve(len);
        util::encode_vi64(VERSION, buf)?;
        util::encode_vi64(self.content_len()? as i64, buf)?;

        Ok(())
    }

    fn decode_base(&mut self, src: &mut BytesMut) -> Result<()> {
        self.base_mut().version = util::decode_vi64(src)?;
        //get payload length, useless right now
        let _payload_length = util::decode_vi64(src)?;
        Ok(())
    }

    //Retrive the header session id, only implemented in stream request
    fn session_id(&self) -> u64 {
        0
    }
    //Retrive the header flag, only implemented in stream request
    fn flag(&self) -> u16 {
        self.base().flag
    }
    //set tenant id
    fn set_tenant_id(&mut self, _tenant_id: Option<u64>) {}
    //set credential
    fn set_credential(&mut self, _credential: &[u8]) {}
    // set request'rpc header into payload
    fn set_header(&mut self, _header: ObRpcPacketHeader) {}
    fn set_trace_id(&mut self, _trace_id: TraceId) {}
    fn set_peer_addr(&mut self, _addr: SocketAddr) {}
}

#[allow(dead_code)]
pub struct DummyObRequest {
    base: BasePayLoad,
    _id: u32,
}

#[allow(dead_code)]
impl DummyObRequest {
    pub fn new(id: u32) -> Self {
        Self {
            base: BasePayLoad::new(),
            _id: id,
        }
    }
}

impl ObPayload for DummyObRequest {
    fn pcode(&self) -> ObTablePacketCode {
        ObTablePacketCode::ExecuteQuery
    }

    fn base(&self) -> &BasePayLoad {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BasePayLoad {
        &mut self.base
    }

    fn content_len(&self) -> Result<usize> {
        Ok(0)
    }
}

impl ProtoEncoder for DummyObRequest {
    fn encode(&self, _buf: &mut BytesMut) -> Result<()> {
        Ok(())
    }
}

impl ProtoDecoder for DummyObRequest {
    fn decode(&mut self, _src: &mut BytesMut) -> Result<()> {
        unimplemented!()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TransportCode {
    Timeout = -20002,
    SendFailure = -20003,
    NullResponse = -20004,
}

#[derive(Debug)]
pub enum ObTablePacket {
    ServerPacket {
        code: Option<ObTablePacketCode>,
        id: i32,
        //channel id
        content: BytesMut,
        header: Box<Option<ObRpcPacketHeader>>,
    },
    TransportPacket {
        error: Error,
        code: TransportCode,
    },
    ClosePoison,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ObTablePacketCodec {
    dlen: i32,
    //data length
    chid: i32, //channel id
}

const API_VERSION: u8 = 1;
static MAGIC_HEADER_FLAG: &[u8] = &[API_VERSION, 0xDB, 0xDB, 0xCE];
static RESERVED: &[u8] = &[0, 0, 0, 0];

impl ObTablePacket {
    pub fn is_close_poison(&self) -> bool {
        matches!(*self, ObTablePacket::ClosePoison)
    }

    /// Retrieve channel id fromn packet
    pub fn channel_id(&self) -> Option<i32> {
        match *self {
            ObTablePacket::ServerPacket {
                code: ref _code,
                content: ref _content,
                id,
                header: ref _header,
            } => Some(id),
            _ => None,
        }
    }
}

impl Default for ObTablePacketCodec {
    fn default() -> ObTablePacketCodec {
        ObTablePacketCodec::new()
    }
}

impl ObTablePacketCodec {
    pub fn new() -> ObTablePacketCodec {
        ObTablePacketCodec { chid: -1, dlen: 0 }
    }
}

impl Encoder<ObTablePacket> for ObTablePacketCodec {
    type Error = io::Error;

    fn encode(&mut self, packet: ObTablePacket, buf: &mut BytesMut) -> Result<()> {
        match packet {
            ObTablePacket::ServerPacket { id, content, .. } => {
                /*
                 * 4bytes  4bytes  4bytes   4bytes
                 * -----------------------------------
                 * | flag |  dlen  | chid | reserved |
                 * -----------------------------------
                 */
                let content_len = content.len();
                buf.reserve(4 + 4 + 4 + 4 + content_len);
                buf.put_slice(MAGIC_HEADER_FLAG);
                buf.put_i32(content_len as i32);
                buf.put_i32(id);
                buf.put_slice(RESERVED);
                buf.extend_from_slice(&content[..]);

                trace!("ObTablePacketCodec::encode buffer {:?}", &content[..]);

                Ok(())
            }
            packet => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("ObTablePacketCodec::encode unexpected packet, packet={packet:?}",),
            )),
        }
    }
}

impl Decoder for ObTablePacketCodec {
    type Error = io::Error;
    type Item = ObTablePacket;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<ObTablePacket>> {
        loop {
            if self.dlen > 0 && buf.len() < self.dlen as usize {
                return Ok(None);
            } else if self.dlen > 0 {
                let data_len = self.dlen;
                let mut content = util::split_buf_to(buf, data_len as usize)?;
                self.dlen = 0;
                let id = self.chid;
                self.chid = -1;

                let mut header = ObRpcPacketHeader::new();

                trace!(
                    "ObTablePacketCodec::decode before decoding header, chid={}, dlen={}.",
                    id,
                    data_len
                );

                header.decode(&mut content)?;

                trace!("ObTablePacketCodec::decode decoding header, chid={}, dlen={}, header={:?}, content={:?}.",
                       id, data_len, header, content.to_vec());

                match header.compress_type {
                    ObCompressType::Invalid => assert_eq!(header.original_len, 0),
                    ObCompressType::None => (), //TODO assert?
                    ObCompressType::Zstd => {
                        let new_content = zstd::stream::decode_all(&content.to_vec()[..])?;
                        assert_eq!(new_content.len(), header.original_len as usize);
                        content.clear();
                        content.reserve(new_content.len());
                        content.extend_from_slice(&new_content);
                        trace!("ObTablePacketCodec::decode decompress content by zstd, chid={}, dlen={}, header={:?}.",
                               id, data_len, header);
                    }
                    _ => panic!(
                        "ObTablePacketCodec::decode unsupported compress type: {:?}",
                        header.compress_type
                    ),
                }

                return Ok(Some(ObTablePacket::ServerPacket {
                    id,
                    code: None,
                    header: Box::new(Some(header)),
                    content,
                }));
            } else {
                if buf.len() < 16 {
                    self.dlen = 0;
                    return Ok(None);
                }
                if &buf[0..4] != MAGIC_HEADER_FLAG {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Invalid magic header {:?}.", &buf[0..4]),
                    ));
                }
                //Skip header
                buf.advance(4);
                {
                    let mut src = Cursor::new(&mut *buf);

                    let dlen = src.get_i32();
                    let chid = src.get_i32();
                    //reserved
                    let _reserved = src.get_i32();
                    self.dlen = dlen;
                    self.chid = chid;
                    trace!("ObTablePacketCodec::decode chid={}, dlen={}", chid, dlen);
                }
                //Skip data length/channel id/reserved
                buf.advance(12);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn encode_decode_header() {
        let header = ObRpcPacketHeader::new();
        let mut buf = BytesMut::new();
        let ret = header.encode(&mut buf);
        assert!(ret.is_ok());
        assert_eq!(RPC_PACKET_HEADER_SIZE_V4, buf.len());
        let mut buf = buf.clone();

        let mut new_header = ObRpcPacketHeader::new();
        let ret = new_header.decode(&mut buf);
        assert!(ret.is_ok());
        assert_eq!(0, buf.len());
        assert_eq!(new_header, header);
    }

    #[test]
    fn encode_decode_codec() {
        let mut codec = ObTablePacketCodec::new();

        let mut content = BytesMut::new();

        let mut header = ObRpcPacketHeader::new();
        header.timeout = 99;
        assert!(header.encode(&mut content).is_ok());
        content.reserve(5);
        content.put_slice(b"hello");

        let packet = ObTablePacket::ServerPacket {
            code: None,
            id: 99,
            content,
            header: Box::new(None),
        };

        let mut buf = BytesMut::new();
        let ret = codec.encode(packet, &mut buf);
        assert!(ret.is_ok());
        assert_eq!(MAGIC_HEADER_FLAG, &buf[0..4]);

        let mut buf = buf.clone();
        let ret = codec.decode(&mut buf);
        assert!(ret.is_ok());
        let ret = ret.unwrap();
        assert!(ret.is_some());
        let ret = ret.unwrap();
        match ret {
            ObTablePacket::ServerPacket {
                code: _,
                content,
                id,
                header,
            } => {
                assert_eq!(id, 99);
                assert_eq!(header.unwrap().timeout, 99);
                assert_eq!(b"hello", &content[..]);
            }
            _ => panic!("decode error"),
        }
        assert_eq!(0, codec.dlen);
        assert_eq!(-1, codec.chid);
    }

    #[test]
    fn test_decompress() {
        let s = "hello world";
        let es = zstd::stream::encode_all(s.as_bytes(), 3).expect("Fail to encode");

        let mut buf = BytesMut::with_capacity(s.len());
        buf.extend_from_slice(&es);

        let ds = zstd::stream::decode_all(&buf.to_vec()[..]).expect("Fail to decode");
        buf.clear();
        buf.reserve(ds.len());
        buf.extend_from_slice(&ds);

        assert_eq!(s.len(), buf.len());
        assert_eq!(
            s,
            String::from_utf8(buf.to_vec()).expect("Fail to decode utf8")
        );
    }

    #[test]
    fn test_trace_id() {
        let trace_id = TraceId(3792882129, 1195690242);
        assert_eq!("YE212C9D1-000000004744C902", format!("{trace_id}"));

        let trace_id = TraceId(2523515528, 1397722356);
        assert_eq!("Y9669CA88-00000000534F8CF4", format!("{trace_id}"));
    }
}
