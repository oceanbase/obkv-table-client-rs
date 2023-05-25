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

pub mod conn_pool;
pub mod protocol;
pub mod proxy;
pub mod util;

use std::{
    collections::HashMap,
    mem,
    net::{SocketAddr, ToSocketAddrs},
    ops::Drop,
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use byteorder::{BigEndian, ByteOrder};
use bytes::BytesMut;
use socket2::{Domain, Protocol, Socket, TcpKeepalive, Type};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpSocket, TcpStream,
    },
    sync::{mpsc, oneshot},
    time::Duration as TokioDuration,
};
use tokio_util::codec::{Decoder, Encoder};
use uuid::Uuid;

use self::protocol::{
    payloads::{ObRpcResultCode, ObTableLoginRequest, ObTableLoginResult},
    ObPayload, ObRpcPacket, ObRpcPacketHeader, ObTablePacket, ObTablePacketCodec, ProtoDecoder,
    ProtoEncoder, TransportCode, HEADER_SIZE,
};
use crate::{
    client::table_client::RuntimesRef,
    error::{CommonErrCode, Error, Error::Common as CommonErr, Result},
    monitors::{prometheus::OBKV_CLIENT_REGISTRY, rpc_metrics::RpcMetrics},
    rpc::{protocol::TraceId, util::checksum::ob_crc64::ObCrc64Sse42},
    runtime::{AbortOnDropMany, JoinHandle, RuntimeRef},
};

lazy_static! {
    pub static ref OBKV_RPC_METRICS: RpcMetrics = {
        let rpc_metrics = RpcMetrics::default();
        rpc_metrics.register(&mut OBKV_CLIENT_REGISTRY.lock().unwrap().registry);
        rpc_metrics
    };
}

type RequestsMap = Arc<Mutex<HashMap<i32, oneshot::Sender<Result<ObTablePacket>>>>>;

const CONN_CONTINUOUS_TIMEOUT_CEILING: usize = 10;

///Send component of OBKV connection.
#[derive(Debug)]
pub struct ConnectionSender {
    sender: mpsc::Sender<ObTablePacket>,
    writer: Option<JoinHandle<Result<()>>>,
    default_runtime: RuntimeRef, // TODO: remove this
}

impl ConnectionSender {
    fn new(
        write_stream: OwnedWriteHalf,
        requests: RequestsMap,
        active: Arc<AtomicBool>,
        sender_runtime: RuntimeRef,
        default_runtime: RuntimeRef,
        channel_capacity: usize,
    ) -> ConnectionSender {
        let (sender, mut receiver): (mpsc::Sender<ObTablePacket>, mpsc::Receiver<ObTablePacket>) =
            mpsc::channel(channel_capacity);
        let mut codec = ObTablePacketCodec::new();

        let writer = sender_runtime.spawn(async move {
                let mut buf = BytesMut::with_capacity(1024);
                let mut write_stream = write_stream;
                let addr = write_stream.peer_addr()?;
                loop {
                    let start = Instant::now();
                    match receiver.recv().await {
                        Some(packet) => {
                            OBKV_RPC_METRICS.observe_rpc_duration(
                                "reveiver_recv_time",
                                start.elapsed(),
                            );
                            if packet.is_close_poison() {
                                break;
                            }
                            // clear the buf for reuse
                            buf.clear();
                            let channel_id = packet.channel_id();
                            match codec.encode(packet, &mut buf) {
                                Ok(()) => {
                                    OBKV_RPC_METRICS.observe_rpc_misc("write_bytes", buf.len() as f64);
                                    let start = Instant::now();
                                    match write_stream.write_all(&buf).await {
                                        Ok(()) => {
                                            OBKV_RPC_METRICS.observe_rpc_duration(
                                                "socket_write",
                                                start.elapsed()
                                            );
                                        }
                                        Err(e) => {
                                            error!(
                                            "Fail to write packet into stream connected to {}, err: {}",
                                            addr, e
                                        );
                                            break;
                                        }
                                    }
                                }
                                Err(e) => match channel_id {
                                    Some(id) => {
                                        Connection::notify_sender(
                                            &requests,
                                            id,
                                            ObTablePacket::TransportPacket {
                                                error: Error::from(e),
                                                code: TransportCode::SendFailure,
                                            },
                                        );
                                    }
                                    None => {
                                        error!("ConnectionSender fail to encode packet: error={}, but channel id not found in packet", e);
                                    }
                                },
                            }
                        }
                        None => {
                            error!("Sender channel has been closed");
                            break;
                        }
                    }
                }

                active.store(false, Ordering::Release);

                if let Err(err) = write_stream.shutdown().await {
                    error!("Fail to close write stream to {addr}, err:{err}");
                }

                drop(receiver);
                Connection::cancel_requests(&requests);

                info!("Close write stream for connection to {}", addr);
                Ok(())
            });

        ConnectionSender {
            sender,
            writer: Some(writer),
            default_runtime,
        }
    }

    ///Performs send of request
    ///
    ///It can fail only when connection gets closed.
    ///Which means OBKV connection is no longer valid.
    pub fn request(&self, message: ObTablePacket) -> Result<()> {
        // TODO: remove block_on with sender.send().await
        self.default_runtime
            .block_on(async move { self.sender.send(message).await.map_err(Self::broken_pipe) })
    }

    fn close(&mut self) -> Result<()> {
        self.request(ObTablePacket::ClosePoison)?;
        let writer = mem::replace(&mut self.writer, None);
        let drop_helper = AbortOnDropMany(vec![writer.unwrap()]);
        drop(drop_helper);
        Ok(())
    }

    #[inline]
    fn broken_pipe<T>(_: T) -> Error {
        CommonErr(
            CommonErrCode::BrokenPipe,
            "No longer able to send messages".to_owned(),
        )
    }
}

/// A Connection to OBKV Server
pub struct Connection {
    //remote addr
    addr: SocketAddr,
    reader: Option<JoinHandle<Result<()>>>,
    reader_signal_sender: mpsc::Sender<()>,
    sender: ConnectionSender,
    requests: RequestsMap,
    continuous_timeout_failures: AtomicUsize,
    continuous_timeout_failures_ceiling: usize,
    credential: Option<Vec<u8>>,
    tenant_id: Option<u64>,
    active: Arc<AtomicBool>,
    id: u32,
    trace_id_counter: AtomicU32,
    load: AtomicUsize,
    // TODO: check unused runtime
    runtimes: RuntimesRef,
}

const OB_MYSQL_MAX_PACKET_LENGTH: usize = 1 << 24;
const READ_BUF_SIZE: usize = 1 << 16;

struct LoadCounter<'a>(&'a AtomicUsize);

impl<'a> LoadCounter<'a> {
    fn new(load: &'a AtomicUsize) -> Self {
        load.fetch_add(1, Ordering::Relaxed);
        Self(load)
    }
}

impl<'a> Drop for LoadCounter<'a> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Connection {
    fn internal_new(
        id: u32,
        addr: SocketAddr,
        stream: TcpStream,
        runtimes: RuntimesRef,
        channel_capacity: usize,
    ) -> Result<Self> {
        let requests: RequestsMap = Arc::new(Mutex::new(HashMap::new()));
        let read_requests = requests.clone();

        let (read_stream, write_stream) = stream.into_split();

        let active = Arc::new(AtomicBool::new(false));
        let read_active = active.clone();
        let (sender, receiver): (mpsc::Sender<()>, mpsc::Receiver<()>) = mpsc::channel(1);

        let join_handle = runtimes.reader_runtime.spawn(async move {
            let addr = read_stream.peer_addr()?;

            Connection::process_reading_data(receiver, read_stream, read_requests.clone(), &addr)
                .await;

            read_active.store(false, Ordering::Release);
            Connection::cancel_requests(&read_requests);

            error!("Close read stream for connection to {}", addr);
            Ok(())
        });

        Ok(Connection {
            addr,
            reader: Some(join_handle),
            sender: ConnectionSender::new(
                write_stream,
                requests.clone(),
                active.clone(),
                runtimes.writer_runtime.clone(),
                runtimes.default_runtime.clone(),
                channel_capacity,
            ),
            requests,
            continuous_timeout_failures: AtomicUsize::new(0),
            continuous_timeout_failures_ceiling: CONN_CONTINUOUS_TIMEOUT_CEILING,
            reader_signal_sender: sender,
            credential: None,
            tenant_id: None,
            active,
            id,
            trace_id_counter: AtomicU32::new(0),
            load: AtomicUsize::new(0),
            runtimes: runtimes.clone(),
        })
    }

    pub fn load(&self) -> usize {
        self.load.load(Ordering::Relaxed)
    }

    async fn process_reading_data(
        mut signal_receiver: mpsc::Receiver<()>,
        mut read_stream: OwnedReadHalf,
        read_requests: RequestsMap,
        addr: &SocketAddr,
    ) {
        let mut codec = ObTablePacketCodec::new();
        let mut read_buf = [0; READ_BUF_SIZE];
        let mut buf = BytesMut::with_capacity(READ_BUF_SIZE);
        loop {
            if let Ok(()) = signal_receiver.try_recv() {
                debug!("Connection::process_reading_data signal_receiver.try_recv() quit");
                break;
            }

            let start = Instant::now();

            match read_stream.read(&mut read_buf).await {
                Ok(size) => {
                    OBKV_RPC_METRICS.observe_rpc_duration("socket_read", start.elapsed());
                    OBKV_RPC_METRICS.observe_rpc_misc("read_bytes", size as f64);

                    if size > 0 {
                        buf.extend_from_slice(&read_buf[0..size]);

                        OBKV_RPC_METRICS.observe_rpc_misc("read_buf_bytes", buf.len() as f64);

                        if buf.len() > OB_MYSQL_MAX_PACKET_LENGTH {
                            debug!(
                                "Connection::process_reading_data too much data in read buffer in \
                                connection to {}",
                                addr
                            );
                        }

                        let start = Instant::now();

                        if !Self::decode_packets(&mut codec, &mut buf, &read_requests, addr) {
                            break;
                        }
                        OBKV_RPC_METRICS
                            .observe_rpc_duration("decode_responses_time", start.elapsed());
                    } else {
                        info!(
                            "Connection::process_reading_data read zero bytes, \
                             the connection to {} was closed by remote.",
                            addr
                        );
                        break;
                    }
                }
                Err(e) => {
                    error!(
                        "Connection::process_reading_data encountered IO error: {} for addr {}.",
                        e, addr
                    );
                    break;
                }
            }
        }
        // TODO: stream close
        drop(read_stream);
    }

    fn cancel_requests(requests: &RequestsMap) {
        let mut requests = requests.lock().unwrap();
        for (_, sender) in requests.drain() {
            if let Err(e) = sender.send(Err(CommonErr(
                CommonErrCode::Rpc,
                "connection reader exits".to_owned(),
            ))) {
                error!("Connection::cancel_requests: fail to send cancel message, err:{e:?}");
            }
        }
    }

    fn decode_packets(
        codec: &mut ObTablePacketCodec,
        buf: &mut BytesMut,
        read_requests: &RequestsMap,
        addr: &SocketAddr,
    ) -> bool {
        let mut decoded = 0;
        loop {
            let start = Instant::now();

            match codec.decode(buf) {
                Ok(Some(response)) => match response {
                    ObTablePacket::ServerPacket {
                        code,
                        header,
                        id,
                        content,
                    } => {
                        trace!("Connection::decode_packets received packet from addr={:?} code={:?}, id={}, header={:?}.",
                               addr, code, id, header);
                        let server_packet = ObTablePacket::ServerPacket {
                            code,
                            header,
                            id,
                            content,
                        };
                        Self::notify_sender(read_requests, id, server_packet);
                        decoded += 1;
                        OBKV_RPC_METRICS.observe_rpc_duration("decode_response", start.elapsed());
                    }

                    error_packet => {
                        error!("Connection::decode_packets receive error packet={:?}, exit reader for addr={}.",
                               error_packet, addr);
                        return false;
                    }
                },
                Ok(None) => {
                    OBKV_RPC_METRICS.observe_rpc_misc("decode_responses", decoded as f64);

                    if decoded > 0 {
                        trace!("Connection::decode_packets decoded {} packets.", decoded);
                    }
                    return true;
                }

                Err(e) => {
                    error!(
                        "Connection::decode_packets fail to decode packet from connection {}, err: {}, exit reader.",
                        addr, e
                    );
                    return false;
                }
            }
        }
    }

    #[inline]
    fn gen_trace_id(&self) -> TraceId {
        // NOTE: TraceId actually uses two numbers with type u32 but its type in
        // protocol is u64  for extensibility.
        TraceId(
            self.id as u64,
            self.trace_id_counter.fetch_add(1, Ordering::Relaxed) as u64,
        )
    }

    fn encode_payload<T: ObPayload>(&self, payload: &T, trace_id: TraceId) -> Result<BytesMut> {
        let start = Instant::now();

        let payload_len = payload.len()?;
        let mut payload_content = BytesMut::with_capacity(payload_len);

        payload.encode(&mut payload_content)?;

        let mut header = ObRpcPacketHeader::new();

        header.set_pcode(payload.pcode().value() as u32);
        header.set_timeout(payload.timeout_millis() * 1000); //us
        header.set_tenant_id(self.tenant_id.unwrap_or(1));
        //session_id and flag now are only valid for stream request
        header.set_session_id(payload.session_id());
        header.set_flag(payload.flag());
        header.set_trace_id(trace_id);

        // compute checksum
        header.set_checksum(ObCrc64Sse42::fast_crc64_sse42_manually(0, &payload_content));

        let packet = ObRpcPacket::new(header, payload_content);

        let mut content = BytesMut::with_capacity(HEADER_SIZE + payload_len);

        packet.encode(&mut content)?;

        OBKV_RPC_METRICS.observe_rpc_duration("encode_payload", start.elapsed());

        Ok(content)
    }

    #[inline]
    fn on_recv_in_time(&self) {
        self.continuous_timeout_failures.store(0, Ordering::Release);
    }

    fn on_recv_timeout(&self) {
        let already_failures = self
            .continuous_timeout_failures
            .fetch_add(1, Ordering::AcqRel);
        if already_failures >= self.continuous_timeout_failures_ceiling {
            error!(
                "Connection::on_recv_timeout: recv timeout failed continuously up to {}, so set connection inactive",
                already_failures + 1
            );
            self.set_active(false);
            Connection::cancel_requests(&self.requests);
        }
    }

    // payload & response should keep Idempotent
    // NOTE: caller should know response wont be be updated when a no-reply request
    // is execute
    pub fn execute<T: ObPayload, R: ObPayload>(
        &self,
        payload: &mut T,
        response: &mut R,
    ) -> Result<()> {
        let _load_counter = LoadCounter::new(&self.load);

        let start = Instant::now();

        let timeout = TokioDuration::from_millis(payload.timeout_millis() as u64);

        payload.set_tenant_id(self.tenant_id);
        if let Some(ref cred) = self.credential {
            payload.set_credential(cred);
        }

        let trace_id = self.gen_trace_id();
        let content = self.encode_payload(payload, trace_id)?;

        let req = ObTablePacket::ServerPacket {
            id: payload.channel_id(),
            code: Some(payload.pcode()),
            header: None,
            content,
        };

        let channel_id = match req.channel_id() {
            None => {
                debug!("Connection::execute: send no reply request");
                self.sender.request(req).map_err(|e| {
                    error!(
                        "Connection::execute fail to send no-reply request, err:{}",
                        e
                    );
                    e
                })?;
                return Ok(());
            }
            Some(id) => id,
        };

        let rx = self.send(req, channel_id)?;

        if payload.timeout_millis() == 0 {
            // no-wait request,return Ok directly
            return Ok(());
        }

        // TODO: remove block_on with rx.await
        let resp = self.runtimes.default_runtime.block_on(async move {
            match tokio::time::timeout(timeout, rx).await {
                Ok(resp) => {
                    self.on_recv_in_time();
                    resp.map_err(|e| {
                        error!(
                        "Connection::execute: fail to fetch rpc response, addr:{}, trace_id:{}, err:{}",
                        self.addr, trace_id, e
                    );
                        e
                    })
                }
                Err(err) => {
                    error!(
                        "Connection::execute: wait for rpc response timeout, addr:{}, trace_id:{}, err:{}",
                        self.addr, trace_id, err
                    );

                    self.on_recv_timeout();
                    return Err(CommonErr(
                        CommonErrCode::Rpc,
                        format!("wait for rpc response timeout, err:{err}"),
                    ));
                }
            }.map_err(|err| CommonErr(CommonErrCode::Rpc, format!("Tokio timeout error: {err:?}")))?
        });

        match resp {
            Ok(ObTablePacket::ServerPacket {
                id: _id,
                header,
                mut content,
                code: _code,
            }) => {
                let header = header.unwrap();
                let server_trace_id = header.trace_id();
                response.set_header(header);
                let mut result_code = ObRpcResultCode::new();
                result_code.decode(&mut content)?;

                if !result_code.is_success() {
                    return Err(CommonErr(
                        CommonErrCode::ObException(result_code.rcode()),
                        format!(
                            "rcode:{:?}, message:{}, addr:{}, trace_id:{trace_id}, server_trace_id:{server_trace_id}",
                            result_code.rcode(),
                            result_code.message(),
                            self.addr,
                        ),
                    ));
                }

                response.decode(&mut content)?;
                OBKV_RPC_METRICS.observe_rpc_duration("execute_payload", start.elapsed());
                Ok(())
            }
            Ok(ObTablePacket::TransportPacket { error, code }) => Err(CommonErr(
                CommonErrCode::Rpc,
                format!("transport code: [{code:?}], error: [{error}]"),
            )),
            _other => Err(CommonErr(
                CommonErrCode::Rpc,
                "Connection::execute unexpected response packet.".parse()?,
            )),
        }
    }

    pub fn connect(
        &mut self,
        tenant_name: &str,
        user_name: &str,
        database_name: &str,
        password: &str,
    ) -> Result<()> {
        self.login(tenant_name, user_name, database_name, password)
    }

    fn login(
        &mut self,
        tenant_name: &str,
        user_name: &str,
        database_name: &str,
        password: &str,
    ) -> Result<()> {
        let start = Instant::now();

        let mut payload = ObTableLoginRequest::new(tenant_name, user_name, database_name, password);

        let mut login_result = ObTableLoginResult::new();

        self.execute(&mut payload, &mut login_result)?;

        debug!("Connection::login, login result {:?}", login_result);

        self.credential = Some(login_result.take_credential());
        self.tenant_id = Some(login_result.tenant_id());

        self.set_active(true);

        OBKV_RPC_METRICS.observe_rpc_duration("login", start.elapsed());

        Ok(())
    }

    // the visibility is just for testing
    #[inline]
    pub fn set_active(&self, active: bool) {
        self.active.store(active, Ordering::Release);
    }

    #[inline]
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    fn notify_sender(read_requests: &RequestsMap, id: i32, packet: ObTablePacket) {
        if let Some(sender) = read_requests.lock().unwrap().remove(&id) {
            if sender.send(Ok(packet)).is_err() {
                trace!("Connection::notify_sender fail to notify, id={}", id);
            }
        } else {
            warn!(
                "Connection sender fail to found sender for request id={}",
                id
            );
        }
    }

    #[inline]
    ///Creates future with default settings that attempts to connect to OBKV.
    ///
    ///Once resolved it spawns tokio's job that handles sends of all messages.
    ///As soon as tokio's runtime will be stopped, the client will be
    /// invalidated.
    ///
    ///For info on default settings see [Builder](struct.Builder.html)
    pub fn new() -> Result<Connection> {
        Builder::new().build()
    }

    /// close the connection
    fn close(&mut self) -> Result<()> {
        if self.reader.is_none() {
            return Ok(());
        }
        self.set_active(false);

        // 1. close writer
        if let Err(e) = self.sender.close() {
            error!("Connection::close fail to close writer, err: {}.", e);
        }

        // 2. close reader
        // TODO: remove block_on
        if let Err(e) = self.runtimes.default_runtime.block_on(async {
            self.reader_signal_sender
                .send(())
                .await
                .map_err(ConnectionSender::broken_pipe)
        }) {
            error!(
                "Connection::close fail to send signal to reader, err: {}.",
                e
            );
        }

        let reader = mem::replace(&mut self.reader, None);

        drop(reader);

        Ok(())
    }

    #[inline]
    ///Performs send of request
    ///
    ///It can fail only when connection gets closed.
    ///Which means OBKV connection is no longer valid.
    pub fn send(
        &self,
        message: ObTablePacket,
        channel_id: i32,
    ) -> Result<oneshot::Receiver<Result<ObTablePacket>>> {
        let (tx, rx) = oneshot::channel();
        self.requests.lock().unwrap().insert(channel_id, tx);
        self.sender.request(message).map_err(|e| {
            error!("Connection::send: fail to send message, err:{}", e);
            self.requests.lock().unwrap().remove(&channel_id);
            e
        })?;

        Ok(rx)
    }

    pub fn remote_addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if let Err(err) = self.close() {
            warn!("Connection::drop fail to close connection, err: {}.", err)
        }
        let mut requests = self.requests.lock().unwrap();
        for (_id, sender) in requests.drain() {
            if let Err(e) = sender
                .send(Ok(ObTablePacket::TransportPacket {
                    error: CommonErr(
                        CommonErrCode::BrokenPipe,
                        "No longer able to send messages".to_owned(),
                    ),
                    code: TransportCode::SendFailure,
                }))
                .map_err(ConnectionSender::broken_pipe)
            {
                error!("Connection::drop fail to notify senders, err: {}.", e);
            }
        }
    }
}

///OBKV Connection builder
#[derive(Clone, Debug)]
pub struct Builder {
    ip: String,
    port: u16,
    connect_timeout: Duration,
    read_timeout: Duration,
    login_timeout: Duration,
    operation_timeout: Duration,

    tenant_name: String,
    user_name: String,
    database_name: String,
    password: String,

    runtimes: Option<RuntimesRef>,

    sender_channel_size: usize,
}

const SOCKET_KEEP_ALIVE_SECS: u64 = 15 * 60;

impl Builder {
    pub fn new() -> Self {
        Self {
            ip: "".to_owned(),
            port: 0,
            connect_timeout: Duration::from_millis(500),
            read_timeout: Duration::from_secs(3),
            login_timeout: Duration::from_secs(3),
            operation_timeout: Duration::from_secs(10),
            tenant_name: "".to_owned(),
            user_name: "".to_owned(),
            database_name: "".to_owned(),
            password: "".to_owned(),
            runtimes: None,
            sender_channel_size: 100,
        }
    }

    pub fn ip(mut self, ip: &str) -> Self {
        self.ip = ip.to_owned();
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    pub fn read_timeout(mut self, timeout: Duration) -> Self {
        self.read_timeout = timeout;
        self
    }

    pub fn login_timeout(mut self, timeout: Duration) -> Self {
        self.login_timeout = timeout;
        self
    }

    pub fn operation_timeout(mut self, timeout: Duration) -> Self {
        self.operation_timeout = timeout;
        self
    }

    pub fn tenant_name(mut self, tenant_name: &str) -> Self {
        self.tenant_name = tenant_name.to_owned();
        self
    }

    pub fn user_name(mut self, user_name: &str) -> Self {
        self.user_name = user_name.to_owned();
        self
    }

    pub fn database_name(mut self, database_name: &str) -> Self {
        self.database_name = database_name.to_owned();
        self
    }

    pub fn password(mut self, password: &str) -> Self {
        self.password = password.to_owned();
        self
    }

    pub fn runtimes(mut self, runtimes: RuntimesRef) -> Self {
        self.runtimes = Some(runtimes);
        self
    }

    pub fn sender_channel_size(mut self, size: usize) -> Self {
        self.sender_channel_size = size;
        self
    }

    pub fn build(self) -> Result<Connection> {
        let uuid = Uuid::new_v4();
        let id = BigEndian::read_u32(uuid.as_bytes());
        self.build_with_id(id)
    }

    pub fn build_with_id(self, id: u32) -> Result<Connection> {
        let addr = (&self.ip[..], self.port).to_socket_addrs()?.next();

        if let Some(addr) = addr {
            let start = Instant::now();

            let socket2_socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;

            socket2_socket.set_nodelay(true)?;
            socket2_socket.set_reuse_address(true)?;
            socket2_socket.set_read_timeout(Some(self.read_timeout))?;
            socket2_socket.set_nonblocking(true)?;
            socket2_socket.set_tcp_keepalive(
                &TcpKeepalive::new().with_time(Duration::from_secs(SOCKET_KEEP_ALIVE_SECS)),
            )?;
            socket2_socket.set_send_buffer_size(READ_BUF_SIZE)?;
            socket2_socket.set_recv_buffer_size(2 * READ_BUF_SIZE)?;

            let tokio_socket = TcpSocket::from_std_stream(socket2_socket.into());

            // TODO: remove block_on
            let stream = self
                .runtimes
                .clone()
                .unwrap()
                .default_runtime
                .block_on(async move {
                    tokio_socket
                        .connect(addr)
                        .await
                        .map_err(|e| {
                            error!("Builder::build fail to connect to {}, err: {}.", addr, e);
                            e
                        })
                        .unwrap()
                });

            debug!("Builder::build succeeds in connecting to {}.", addr);

            let result = Connection::internal_new(
                id,
                addr,
                stream,
                self.runtimes.unwrap(),
                self.sender_channel_size,
            );

            OBKV_RPC_METRICS.observe_rpc_duration("connect", start.elapsed());

            result
        } else {
            Err(CommonErr(
                CommonErrCode::InvalidServerAddr,
                "Invalid observer address".to_owned(),
            ))
        }
    }
}

#[cfg(test)]
mod test {
    use bytes::{BufMut, BytesMut};

    use super::*;

    const TEST_SERVER_IP: &str = "127.0.0.1";
    const TEST_SERVER_PORT: u16 = 2882;

    fn gen_test_server_packet(id: i32) -> ObTablePacket {
        let mut content = BytesMut::with_capacity(5);
        content.put_slice(&b"hello"[..]);
        ObTablePacket::ServerPacket {
            code: None,
            id,
            content,
            header: None,
        }
    }

    #[test]
    #[ignore]
    fn test_connect() {
        let packet = gen_test_server_packet(100);

        let mut builder = Builder::new();
        builder = builder.ip(TEST_SERVER_IP).port(TEST_SERVER_PORT);

        let mut conn: Connection = builder.build().expect("Create OBKV Client");

        let channel_id = packet.channel_id().unwrap();
        let res = conn
            .send(packet, channel_id)
            .expect("fail to send request")
            .try_recv();
        assert!(res.is_ok());
        assert!(conn.close().is_ok());
    }
}
