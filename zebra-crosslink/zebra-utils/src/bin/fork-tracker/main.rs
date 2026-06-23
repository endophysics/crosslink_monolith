//! Crawl Crosslink testnet peers and cache a network-wide fork tree.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Write as _,
    net::SocketAddr,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use color_eyre::eyre::{ensure, eyre, Report, Result, WrapErr};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use structopt::StructOpt;
use tokio::task::JoinSet;
use tokio::time::{interval, timeout, MissedTickBehavior};
use tower::{Service, ServiceExt};
use tracing::{info, warn};

use zebra_chain::{
    block::{self, Height},
    parameters::{testnet, Magic, Network},
    serialization::{ZcashDeserialize, ZcashSerialize},
};
use zebra_network::{connect_isolated_tcp_direct, Request, Response};
use zebra_node_services::rpc_client::RpcRequestClient;
use zebra_utils::init_tracing;

#[derive(Debug, StructOpt)]
struct Args {
    /// SQLite database path.
    #[structopt(long, default_value = "fork-tracker.sqlite")]
    db: PathBuf,

    #[structopt(subcommand)]
    command: Command,
}

#[derive(Debug, StructOpt)]
enum Command {
    /// Crawl peers, discover more peers, and cache observed headers.
    Crawl(CrawlArgs),

    /// Continuously crawl peers, discover new peers, and refresh observed chains.
    Daemon(DaemonArgs),

    /// Export the cached fork tree.
    Export(ExportArgs),
}

#[derive(Clone, Debug, StructOpt)]
struct CrawlArgs {
    /// Local Zebra JSON-RPC address used for genesis hash and getpeerinfo seeds.
    #[structopt(long, default_value = "127.0.0.1:8232")]
    rpc_addr: SocketAddr,

    /// Seed peer address in IP:port form. Can be supplied multiple times.
    #[structopt(long)]
    peer: Vec<SocketAddr>,

    /// Crosslink network profile: crosslink-testnet-0, crosslink-testnet, or crosslink-regtestnet.
    #[structopt(long, default_value = "crosslink-testnet-0")]
    network: NetworkArg,

    /// User agent sent on isolated remote peer connections.
    #[structopt(long, default_value = "")]
    user_agent: String,

    /// Timeout for each remote peer request, in seconds.
    #[structopt(long, default_value = "10")]
    connect_timeout_secs: u64,

    /// Maximum peers to crawl in this run.
    #[structopt(long, default_value = "500")]
    max_peers: usize,

    /// Maximum peer network crawls to run concurrently. SQLite writes remain serialized.
    #[structopt(long, default_value = "4")]
    concurrency: usize,

    /// Maximum header observations to store per peer in this run.
    #[structopt(long, default_value = "10000")]
    max_headers_per_peer: usize,

    /// Discovery/crawl passes over the peer frontier.
    #[structopt(long, default_value = "3")]
    rounds: usize,

    /// Allow peers discovered via getaddr to include loopback/private/link-local addresses.
    ///
    /// Explicit --peer values and local getpeerinfo seeds are always accepted because they are
    /// operator/local-node controlled. This flag only affects untrusted remote getaddr results.
    #[structopt(long)]
    allow_private_discovered_peers: bool,
}

#[derive(Debug, StructOpt)]
struct DaemonArgs {
    #[structopt(flatten)]
    crawl: CrawlArgs,

    /// Seconds to wait between crawl iterations.
    ///
    /// Zebra peers cache getaddr responses for around 10 minutes, so the default avoids hammering
    /// the same peers while still refreshing chain tips regularly.
    #[structopt(long, default_value = "600")]
    crawl_interval_secs: u64,

    /// Stop after this many crawl iterations. Omit to run until interrupted.
    #[structopt(long)]
    max_iterations: Option<usize>,
}

#[derive(Debug, StructOpt)]
struct ExportArgs {
    /// Export format: json, dot, or html.
    #[structopt(long, default_value = "json")]
    format: ExportFormat,

    /// Include every header in DOT output. By default DOT keeps forks, tips, genesis, and sampled checkpoints.
    #[structopt(long)]
    dot_full: bool,

    /// Height interval for sampled checkpoints in compact DOT output.
    #[structopt(long, default_value = "1000")]
    dot_sample_interval: u32,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum ExportFormat {
    Json,
    Dot,
    Html,
}

impl FromStr for ExportFormat {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "json" => Ok(Self::Json),
            "dot" => Ok(Self::Dot),
            "html" => Ok(Self::Html),
            _ => Err(format!(
                "invalid export format {value:?}; expected json, dot, or html"
            )),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum NetworkArg {
    CrosslinkTestnet0,
    CrosslinkTestnet,
    CrosslinkRegtestnet,
}

impl FromStr for NetworkArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "crosslink" | "crosslink-testnet-0" | "clt0" => Ok(Self::CrosslinkTestnet0),
            "crosslink-testnet" | "cltn" => Ok(Self::CrosslinkTestnet),
            "crosslink-regtestnet" | "crosslink-regtest" | "clrn" => {
                Ok(Self::CrosslinkRegtestnet)
            }
            _ => Err(format!(
                "invalid Crosslink network {value:?}; expected crosslink-testnet-0, crosslink-testnet, or crosslink-regtestnet"
            )),
        }
    }
}

impl NetworkArg {
    fn to_network(self) -> Network {
        match self {
            Self::CrosslinkTestnet0 => crosslink_testnet(Magic([67, 108, 84, 48])),
            Self::CrosslinkTestnet => crosslink_testnet(Magic([67, 108, 84, 110])),
            Self::CrosslinkRegtestnet => crosslink_testnet(Magic([67, 108, 82, 110])),
        }
    }
}

fn crosslink_testnet(network_magic: Magic) -> Network {
    testnet::Parameters::build()
        .with_network_magic(network_magic)
        .with_slow_start_interval(Height(0))
        .to_network()
}

#[derive(Copy, Clone, Debug)]
struct PeerTip {
    height: Height,
    hash: block::Hash,
}

#[derive(Copy, Clone, Debug)]
struct Anchor {
    height: Height,
    hash: block::Hash,
}

#[derive(Debug)]
struct DotHeader {
    hash: String,
    parent_hash: String,
    height: Option<u32>,
    current_peer_count: i64,
    observed_peer_count: i64,
    child_count: i64,
    is_tip: bool,
}

struct HeaderBatch {
    headers: Vec<block::CountedHeader>,
    reached_limit: bool,
    request_error: Option<String>,
}

struct PeerCrawlTaskResult {
    peer_id: i64,
    peer_addr: SocketAddr,
    fallback_anchor: Anchor,
    result: Result<PeerNetworkCrawl>,
}

struct PeerNetworkCrawl {
    user_agent: String,
    services: String,
    advertised_height: Height,
    discovered: Vec<SocketAddr>,
    getaddr_error: Option<String>,
    headers: HeaderBatch,
}

#[derive(Clone, Debug)]
struct RpcConnectedPeer {
    addr: String,
    socket_addr: Option<SocketAddr>,
    inbound: bool,
    ready: bool,
    user_agent: String,
    services: String,
    advertised_height: Height,
}

#[tokio::main]
#[allow(clippy::print_stdout, clippy::print_stderr)]
async fn main() -> Result<()> {
    init_tracing();
    color_eyre::install()?;

    let args = Args::from_args();
    let db = Connection::open(&args.db)
        .wrap_err_with(|| format!("failed to open SQLite database {}", args.db.display()))?;
    init_db(&db)?;

    match args.command {
        Command::Crawl(crawl_args) => crawl(&db, crawl_args).await,
        Command::Daemon(daemon_args) => daemon(&db, daemon_args).await,
        Command::Export(export_args) => export(&db, export_args),
    }
}

async fn daemon(db: &Connection, args: DaemonArgs) -> Result<()> {
    validate_daemon_args(&args)?;

    let mut crawl_interval = interval(Duration::from_secs(args.crawl_interval_secs));
    crawl_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    crawl_interval.tick().await;
    let mut iteration = 0usize;

    info!(
        crawl_interval_secs = args.crawl_interval_secs,
        max_iterations = ?args.max_iterations,
        "starting fork-tracker daemon"
    );
    eprintln!(
        "INFO starting fork-tracker daemon: crawl_interval_secs={} max_iterations={:?}",
        args.crawl_interval_secs, args.max_iterations
    );

    loop {
        iteration = iteration.saturating_add(1);
        info!(iteration, "daemon crawl iteration starting");
        eprintln!("INFO daemon crawl iteration {iteration} starting");

        match crawl(db, args.crawl.clone()).await {
            Ok(()) => {
                info!(iteration, "daemon crawl iteration finished");
                eprintln!("INFO daemon crawl iteration {iteration} finished");
            }
            Err(error) => {
                let error = format_error_chain(&error);
                warn!(iteration, %error, "daemon crawl iteration failed");
                eprintln!("WARN daemon crawl iteration {iteration} failed: {error}");
            }
        }

        if args
            .max_iterations
            .is_some_and(|max_iterations| iteration >= max_iterations)
        {
            info!(iteration, "daemon exiting after max iterations");
            eprintln!("INFO daemon exiting after {iteration} crawl iterations");
            return Ok(());
        }

        info!(
            sleep_secs = args.crawl_interval_secs,
            "daemon sleeping before next crawl"
        );
        eprintln!(
            "INFO daemon sleeping {} seconds before next crawl",
            args.crawl_interval_secs
        );

        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result.wrap_err("failed to listen for Ctrl-C")?;
                info!("daemon received Ctrl-C; exiting");
                eprintln!("INFO daemon received Ctrl-C; exiting");
                return Ok(());
            }
            _ = crawl_interval.tick() => {}
        }
    }
}

fn validate_daemon_args(args: &DaemonArgs) -> Result<()> {
    ensure!(
        args.crawl_interval_secs > 0,
        "--crawl-interval-secs must be greater than 0"
    );
    Ok(())
}

fn init_db(db: &Connection) -> Result<()> {
    db.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS peers (
            id INTEGER PRIMARY KEY,
            addr TEXT NOT NULL UNIQUE,
            first_seen INTEGER NOT NULL,
            last_seen INTEGER NOT NULL,
            last_success INTEGER,
            last_failure INTEGER,
            user_agent TEXT,
            services TEXT,
            advertised_height INTEGER,
            status TEXT NOT NULL DEFAULT 'discovered'
        );

        CREATE TABLE IF NOT EXISTS peer_addresses (
            source_peer_id INTEGER,
            addr TEXT NOT NULL,
            first_seen INTEGER NOT NULL,
            last_seen INTEGER NOT NULL,
            UNIQUE(source_peer_id, addr)
        );

        CREATE TABLE IF NOT EXISTS headers (
            hash TEXT PRIMARY KEY,
            parent_hash TEXT NOT NULL,
            height INTEGER,
            raw_header_hex TEXT NOT NULL,
            first_seen INTEGER NOT NULL,
            last_seen INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS peer_header_observations (
            peer_id INTEGER NOT NULL,
            height INTEGER NOT NULL,
            hash TEXT NOT NULL,
            crawl_session_id INTEGER NOT NULL,
            observed_at INTEGER NOT NULL,
            PRIMARY KEY(peer_id, height, hash, crawl_session_id),
            FOREIGN KEY(peer_id) REFERENCES peers(id)
        );

        CREATE TABLE IF NOT EXISTS peer_best_path (
            peer_id INTEGER NOT NULL,
            height INTEGER NOT NULL,
            hash TEXT NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY(peer_id, height),
            FOREIGN KEY(peer_id) REFERENCES peers(id)
        );

        CREATE TABLE IF NOT EXISTS peer_tips (
            peer_id INTEGER PRIMARY KEY,
            tip_hash TEXT NOT NULL,
            tip_height INTEGER NOT NULL,
            observed_at INTEGER NOT NULL,
            FOREIGN KEY(peer_id) REFERENCES peers(id)
        );

        CREATE TABLE IF NOT EXISTS peer_events (
            id INTEGER PRIMARY KEY,
            peer_id INTEGER,
            kind TEXT NOT NULL,
            details_json TEXT NOT NULL,
            observed_at INTEGER NOT NULL,
            FOREIGN KEY(peer_id) REFERENCES peers(id)
        );

        CREATE TABLE IF NOT EXISTS crawl_sessions (
            id INTEGER PRIMARY KEY,
            started_at INTEGER NOT NULL,
            finished_at INTEGER,
            peer_count INTEGER NOT NULL DEFAULT 0,
            notes TEXT
        );

        CREATE INDEX IF NOT EXISTS headers_parent_hash ON headers(parent_hash);
        CREATE INDEX IF NOT EXISTS peer_header_hash ON peer_header_observations(hash);
        CREATE INDEX IF NOT EXISTS peer_best_path_hash ON peer_best_path(hash);
        ",
    )?;

    Ok(())
}

async fn crawl(db: &Connection, args: CrawlArgs) -> Result<()> {
    ensure!(args.max_peers > 0, "--max-peers must be greater than 0");
    ensure!(args.concurrency > 0, "--concurrency must be greater than 0");
    ensure!(
        args.max_headers_per_peer > 0,
        "--max-headers-per-peer must be greater than 0"
    );
    ensure!(args.rounds > 0, "--rounds must be greater than 0");

    let network = args.network.to_network();
    info!(
        rpc_addr = %args.rpc_addr,
        network = ?args.network,
        explicit_peer_count = args.peer.len(),
        max_peers = args.max_peers,
        concurrency = args.concurrency,
        max_headers_per_peer = args.max_headers_per_peer,
        rounds = args.rounds,
        allow_private_discovered_peers = args.allow_private_discovered_peers,
        "starting crawl session"
    );
    eprintln!(
        "INFO starting crawl session: rpc_addr={} network={:?} explicit_peer_count={} max_peers={} concurrency={} max_headers_per_peer={} rounds={} allow_private_discovered_peers={}",
        args.rpc_addr,
        args.network,
        args.peer.len(),
        args.max_peers,
        args.concurrency,
        args.max_headers_per_peer,
        args.rounds,
        args.allow_private_discovered_peers,
    );

    let rpc_client = RpcRequestClient::new(args.rpc_addr);
    let genesis_hash = local_block_hash(&rpc_client, Height(0))
        .await
        .wrap_err("failed to query local genesis hash")?;
    ensure_genesis_header(db, genesis_hash)?;
    info!(%genesis_hash, "loaded local genesis hash");
    eprintln!("INFO loaded local genesis hash {genesis_hash}");

    let session_id = start_crawl_session(db)?;
    info!(session_id, "created crawl session");

    info!(
        session_id,
        rpc_addr = %args.rpc_addr,
        max_headers = args.max_headers_per_peer,
        "starting local chain import"
    );
    eprintln!(
        "INFO crawl session {session_id}: importing local chain from {} max_headers={}",
        args.rpc_addr, args.max_headers_per_peer,
    );

    import_local_chain(
        db,
        session_id,
        &rpc_client,
        args.rpc_addr,
        genesis_hash,
        args.max_headers_per_peer,
    )
    .await
    .wrap_err("failed to import local chain")?;

    match crawl_rpc_connected_peers(
        db,
        session_id,
        &rpc_client,
        genesis_hash,
        args.max_headers_per_peer,
    )
    .await
    {
        Ok(crawled) => {
            info!(
                session_id,
                crawled, "crawled connected peers through local RPC diagnostics"
            );
            eprintln!(
                "INFO crawl session {session_id}: crawled {crawled} connected peers through local RPC diagnostics",
            );
        }
        Err(error) => {
            let error = format_error_chain(&error);
            warn!(session_id, %error, "connected peer diagnostics unavailable");
            eprintln!(
                "WARN crawl session {session_id}: connected peer diagnostics unavailable: {error}",
            );
        }
    }

    let mut queue = VecDeque::new();
    let mut queued = HashSet::new();

    for peer in args.peer.iter().copied() {
        enqueue_peer(db, &mut queue, &mut queued, None, peer)?;
    }

    let local_rpc_peers = local_rpc_peers(&rpc_client).await?;
    let local_rpc_peer_count = local_rpc_peers.len();
    for peer in local_rpc_peers {
        enqueue_peer(db, &mut queue, &mut queued, None, peer)?;
    }

    let stored_peers = stored_peer_addrs(db)?;
    let stored_peer_count = stored_peers.len();
    for peer in stored_peers {
        enqueue_peer(db, &mut queue, &mut queued, None, peer)?;
    }

    info!(
        session_id,
        explicit_peer_count = args.peer.len(),
        local_rpc_peer_count,
        stored_peer_count,
        initial_queue_len = queue.len(),
        "seeded crawl queue"
    );
    eprintln!(
        "INFO crawl session {session_id}: seeded queue initial_queue_len={} explicit={} local_rpc={} stored={}",
        queue.len(),
        args.peer.len(),
        local_rpc_peer_count,
        stored_peer_count,
    );

    if queue.is_empty() {
        return Err(eyre!(
            "no seed peers found; pass --peer or run a local node with getpeerinfo peers"
        ));
    }

    let mut crawled = 0usize;
    for round in 0..args.rounds {
        let round_len = queue.len();
        info!(
            session_id,
            round = round + 1,
            round_len,
            crawled,
            queued_total = queued.len(),
            "starting crawl round"
        );
        eprintln!(
            "INFO crawl session {session_id}: round {} starting round_len={} crawled={} queued_total={}",
            round + 1,
            round_len,
            crawled,
            queued.len(),
        );

        let mut round_successes = 0usize;
        let mut round_failures = 0usize;
        let mut round_discovered = 0usize;
        let mut round_enqueued = 0usize;
        let mut round_ignored = 0usize;
        let mut scheduled_this_round = 0usize;
        let mut in_flight = JoinSet::new();

        while scheduled_this_round < round_len || !in_flight.is_empty() {
            while scheduled_this_round < round_len
                && in_flight.len() < args.concurrency
                && crawled < args.max_peers
            {
                let Some(peer_addr) = queue.pop_front() else {
                    break;
                };

                let peer_id = upsert_peer_seen(db, peer_addr)?;
                let locator = peer_locator(db, peer_id, genesis_hash)?;
                let task_args = args.clone();
                let task_network = network.clone();
                let locator_anchor_height = locator.anchor.height.0;
                let locator_anchor_hash = locator.anchor.hash;
                let locator_hash_count = locator.hashes.len();

                info!(
                    session_id,
                    peer_id,
                    %peer_addr,
                    locator_anchor_height,
                    locator_anchor_hash = %locator_anchor_hash,
                    locator_hash_count,
                    in_flight = in_flight.len(),
                    "scheduling peer crawl"
                );
                eprintln!(
                    "INFO crawl session {session_id}: scheduling peer {peer_addr} peer_id={peer_id} round={} scheduled={}/{} crawled={} in_flight={} locator_anchor={}@{} locator_hashes={}",
                    round + 1,
                    scheduled_this_round + 1,
                    round_len,
                    crawled + 1,
                    in_flight.len(),
                    locator_anchor_hash,
                    locator_anchor_height,
                    locator_hash_count,
                );

                in_flight.spawn(async move {
                    let fallback_anchor = locator.anchor;
                    let result =
                        crawl_peer_network(&task_args, &task_network, peer_addr, locator).await;
                    PeerCrawlTaskResult {
                        peer_id,
                        peer_addr,
                        fallback_anchor,
                        result,
                    }
                });

                crawled += 1;
                scheduled_this_round += 1;
            }

            let Some(joined) = in_flight.join_next().await else {
                break;
            };

            let task_result = joined.wrap_err("peer crawl task panicked or was cancelled")?;
            let peer_id = task_result.peer_id;
            let peer_addr = task_result.peer_addr;
            let fallback_anchor = task_result.fallback_anchor;

            match task_result.result {
                Ok(peer_crawl) => {
                    round_successes += 1;
                    let discovered_count = peer_crawl.discovered.len();
                    round_discovered += discovered_count;
                    let queued_len_before_discovered = queued.len();

                    update_peer_success(
                        db,
                        peer_id,
                        &peer_crawl.user_agent,
                        &peer_crawl.services,
                        peer_crawl.advertised_height,
                    )?;

                    if let Some(error) = peer_crawl.getaddr_error.as_deref() {
                        record_peer_event(
                            db,
                            Some(peer_id),
                            "getaddr_error",
                            json!({ "error": error }),
                        )?;
                    }

                    let header_count = peer_crawl.headers.headers.len();
                    let reached_limit = peer_crawl.headers.reached_limit;
                    let header_request_error = peer_crawl.headers.request_error.clone();
                    persist_headers(db, session_id, peer_id, fallback_anchor, peer_crawl.headers)?;

                    for discovered_addr in peer_crawl.discovered {
                        if !args.allow_private_discovered_peers
                            && !is_safe_remote_discovered_addr(discovered_addr)
                        {
                            round_ignored += 1;
                            record_peer_event(
                                db,
                                Some(peer_id),
                                "ignored_unsafe_discovered_peer",
                                json!({ "addr": discovered_addr.to_string() }),
                            )?;
                            continue;
                        }

                        enqueue_peer(db, &mut queue, &mut queued, Some(peer_id), discovered_addr)?;
                    }
                    let enqueued = queued.len().saturating_sub(queued_len_before_discovered);
                    let skipped = discovered_count.saturating_sub(enqueued);
                    round_enqueued += enqueued;
                    info!(
                        session_id,
                        peer_id,
                        %peer_addr,
                        discovered_count,
                        enqueued,
                        skipped,
                        header_count,
                        reached_limit,
                        header_request_error = header_request_error.is_some(),
                        queue_len = queue.len(),
                        in_flight = in_flight.len(),
                        "finished peer crawl result"
                    );
                    eprintln!(
                        "INFO finished peer {peer_addr}: discovered={} headers={} reached_limit={} header_request_error={}",
                        discovered_count,
                        header_count,
                        reached_limit,
                        header_request_error.is_some(),
                    );
                }
                Err(error) => {
                    let error = format_error_chain(&error);
                    record_peer_failure(db, peer_id, &error)?;
                    warn!(session_id, peer_id, %peer_addr, %error, "peer crawl failed");
                    eprintln!("WARN peer {peer_addr} failed: {error}");
                    round_failures += 1;
                }
            }
        }

        info!(
            session_id,
            round = round + 1,
            crawled,
            round_successes,
            round_failures,
            round_discovered,
            round_enqueued,
            round_ignored,
            queue_len = queue.len(),
            "finished crawl round"
        );
        eprintln!(
            "INFO crawl session {session_id}: round {} finished crawled={} successes={} failures={} discovered={} enqueued={} ignored={} queue_len={}",
            round + 1,
            crawled,
            round_successes,
            round_failures,
            round_discovered,
            round_enqueued,
            round_ignored,
            queue.len(),
        );

        if queue.is_empty() {
            info!(
                session_id,
                round = round + 1,
                crawled,
                "crawl queue exhausted; ending session"
            );
            eprintln!(
                "INFO crawl session {session_id}: queue exhausted after round {} crawled={}",
                round + 1,
                crawled,
            );
            break;
        }

        if crawled >= args.max_peers {
            info!(
                session_id,
                crawled, "finishing crawl session after reaching max peers"
            );
            finish_crawl_session(db, session_id, crawled, "max peers reached")?;
            return Ok(());
        }
    }

    info!(session_id, crawled, "finishing crawl session");
    finish_crawl_session(db, session_id, crawled, "finished")?;
    Ok(())
}

async fn crawl_peer_network(
    args: &CrawlArgs,
    network: &Network,
    peer_addr: SocketAddr,
    locator: Locator,
) -> Result<PeerNetworkCrawl> {
    info!(%peer_addr, "crawling peer");
    eprintln!("INFO crawling peer {peer_addr}");
    let mut peer = connect_peer(
        network,
        peer_addr,
        args.user_agent.clone(),
        args.connect_timeout_secs,
    )
    .await?;

    info!(
        %peer_addr,
        user_agent = %peer.connection_info.remote.user_agent,
        services = ?peer.connection_info.remote.services,
        advertised_height = ?peer.connection_info.remote.start_height,
        "connected to peer"
    );
    eprintln!(
        "INFO connected to peer {peer_addr}: user_agent={} services={:?} advertised_height={}",
        peer.connection_info.remote.user_agent,
        peer.connection_info.remote.services,
        peer.connection_info.remote.start_height.0,
    );

    let user_agent = peer.connection_info.remote.user_agent.clone();
    let services = format!("{:?}", peer.connection_info.remote.services);
    let advertised_height = peer.connection_info.remote.start_height;

    let (discovered, getaddr_error) = match request_peers(&mut peer).await {
        Ok(discovered) => (discovered, None),
        Err(error) => {
            let error = format_error_chain(&error);
            warn!(%peer_addr, %error, "getaddr request failed");
            eprintln!("WARN getaddr request failed for {peer_addr}: {error}");
            (Vec::new(), Some(error))
        }
    };
    eprintln!(
        "INFO peer {peer_addr}: getaddr discovered {} candidate peers",
        discovered.len()
    );

    info!(
        %peer_addr,
        locator_anchor_height = locator.anchor.height.0,
        locator_anchor_hash = %locator.anchor.hash,
        locator_hash_count = locator.hashes.len(),
        discovered_count = discovered.len(),
        "requesting peer headers"
    );
    let batch = request_headers(
        &mut peer,
        peer_addr,
        locator.hashes,
        args.max_headers_per_peer,
    )
    .await?;

    Ok(PeerNetworkCrawl {
        user_agent,
        services,
        advertised_height,
        discovered,
        getaddr_error,
        headers: batch,
    })
}

struct Locator {
    hashes: Vec<block::Hash>,
    anchor: Anchor,
}

fn peer_locator(db: &Connection, peer_id: i64, genesis_hash: block::Hash) -> Result<Locator> {
    let mut statement = db.prepare(
        "SELECT height, hash FROM peer_best_path WHERE peer_id = ?1 ORDER BY height DESC",
    )?;
    let rows = statement.query_map(params![peer_id], |row| {
        let height: u32 = row.get(0)?;
        let hash: String = row.get(1)?;
        Ok((Height(height), hash))
    })?;

    let path: Vec<(Height, block::Hash)> = rows
        .map(|row| {
            let (height, hash) = row?;
            parse_hash(&hash)
                .map(|hash| (height, hash))
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(error.into()))
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;

    if path.is_empty() {
        return Ok(Locator {
            hashes: vec![genesis_hash],
            anchor: Anchor {
                height: Height(0),
                hash: genesis_hash,
            },
        });
    }

    let mut hashes = Vec::new();
    let mut step = 1usize;
    let mut index = 0usize;
    while index < path.len() {
        hashes.push(path[index].1);
        if hashes.len() > 10 {
            step = step.saturating_mul(2);
        }
        index = index.saturating_add(step);
    }

    if !hashes.contains(&genesis_hash) {
        hashes.push(genesis_hash);
    }

    Ok(Locator {
        hashes,
        anchor: Anchor {
            height: path[0].0,
            hash: path[0].1,
        },
    })
}

async fn request_peers(peer: &mut zebra_network::Client) -> Result<Vec<SocketAddr>> {
    match peer
        .ready()
        .await
        .map_err(|error| eyre!(error.to_string()))?
        .call(Request::Peers)
        .await
        .map_err(|error| eyre!(error.to_string()))?
    {
        Response::Peers(peers) => Ok(peers
            .into_iter()
            .map(|peer| peer.addr().remove_socket_addr_privacy())
            .collect()),
        response => Err(eyre!(
            "unexpected response to Peers request: {}",
            response.command()
        )),
    }
}

async fn request_headers(
    peer: &mut zebra_network::Client,
    peer_addr: SocketAddr,
    known_blocks: Vec<block::Hash>,
    max_headers: usize,
) -> Result<HeaderBatch> {
    let mut collected = Vec::new();
    let initial_locator_len = known_blocks.len();
    let mut locator = known_blocks;
    let mut reached_limit = false;
    let mut request_round_trips = 0usize;
    let mut last_hash = None;

    while collected.len() < max_headers {
        request_round_trips = request_round_trips.saturating_add(1);
        let response = match peer.ready().await.map_err(|error| eyre!(error.to_string())) {
            Ok(service) => service
                .call(Request::FindHeaders {
                    known_blocks: locator.clone(),
                    stop: None,
                })
                .await
                .map_err(|error| eyre!(error.to_string())),
            Err(error) => Err(error),
        };

        let headers = match response {
            Ok(response) => match response {
                Response::Nil => Vec::new(),
                Response::BlockHeaders(headers) => headers,
                response => {
                    let error = eyre!("unexpected response to FindHeaders: {}", response.command());
                    return finish_header_request_after_error(collected, reached_limit, error);
                }
            },
            Err(error) => {
                return finish_header_request_after_error(collected, reached_limit, error)
            }
        };

        if headers.is_empty() {
            info!(
                %peer_addr,
                request_round_trips,
                headers_collected = collected.len(),
                "peer returned no additional headers"
            );
            eprintln!(
                "INFO peer {peer_addr}: no additional headers after {} round trips collected={}",
                request_round_trips,
                collected.len(),
            );
            break;
        }

        let remaining = max_headers - collected.len();
        let header_count = headers.len();
        let taken_headers: Vec<_> = headers.into_iter().take(remaining).collect();
        reached_limit =
            header_count > remaining || collected.len() + taken_headers.len() >= max_headers;
        let batch_last_hash = taken_headers
            .last()
            .expect("taken headers is not empty")
            .header
            .hash();
        last_hash = Some(batch_last_hash);
        collected.extend(taken_headers);
        info!(
            %peer_addr,
            request_round_trips,
            batch_header_count = header_count,
            headers_collected = collected.len(),
            max_headers,
            reached_limit,
            last_hash = %batch_last_hash,
            "received header batch"
        );
        eprintln!(
            "INFO peer {peer_addr}: header batch round={} batch={} collected={}/{} reached_limit={} last_hash={}",
            request_round_trips,
            header_count,
            collected.len(),
            max_headers,
            reached_limit,
            batch_last_hash,
        );
        locator = vec![batch_last_hash];
    }

    info!(
        initial_locator_len,
        %peer_addr,
        request_round_trips,
        headers_collected = collected.len(),
        reached_limit,
        last_hash = ?last_hash,
        "finished header requests"
    );

    Ok(HeaderBatch {
        headers: collected,
        reached_limit,
        request_error: None,
    })
}

fn finish_header_request_after_error(
    collected: Vec<block::CountedHeader>,
    reached_limit: bool,
    error: Report,
) -> Result<HeaderBatch> {
    if collected.is_empty() {
        return Err(error);
    }

    let error = format_error_chain(&error);
    warn!(
        headers_collected = collected.len(),
        reached_limit,
        %error,
        "returning partial headers after request failure"
    );

    Ok(HeaderBatch {
        headers: collected,
        reached_limit,
        request_error: Some(error),
    })
}

async fn crawl_rpc_connected_peers(
    db: &Connection,
    session_id: i64,
    client: &RpcRequestClient,
    genesis_hash: block::Hash,
    max_headers: usize,
) -> Result<usize> {
    let peers = local_rpc_connected_peers(client).await?;
    let ready_count = peers.iter().filter(|peer| peer.ready).count();
    info!(
        session_id,
        connected_peer_count = peers.len(),
        ready_count,
        "loaded connected peers from local RPC diagnostics"
    );
    eprintln!(
        "INFO crawl session {session_id}: connected peer diagnostics returned {} peers ready={}",
        peers.len(),
        ready_count,
    );

    let mut crawled = 0usize;
    for peer in peers.into_iter().filter(|peer| peer.ready) {
        let peer_id = upsert_peer_label(db, &peer.addr)?;
        update_peer_success(
            db,
            peer_id,
            &peer.user_agent,
            &peer.services,
            peer.advertised_height,
        )?;

        let locator = peer_locator(db, peer_id, genesis_hash)?;
        info!(
            session_id,
            peer_id,
            peer_addr = %peer.addr,
            socket_addr = ?peer.socket_addr,
            inbound = peer.inbound,
            locator_anchor_height = locator.anchor.height.0,
            locator_anchor_hash = %locator.anchor.hash,
            locator_hash_count = locator.hashes.len(),
            "requesting connected peer headers through RPC diagnostics"
        );
        eprintln!(
            "INFO connected peer {}: requesting headers over RPC diagnostics locator={}@{} locator_hashes={}",
            peer.addr,
            locator.anchor.hash,
            locator.anchor.height.0,
            locator.hashes.len(),
        );

        match request_rpc_peer_headers(client, &peer.addr, locator.hashes, max_headers).await {
            Ok(batch) => {
                let header_count = batch.headers.len();
                let reached_limit = batch.reached_limit;
                let header_request_error = batch.request_error.is_some();
                persist_headers(db, session_id, peer_id, locator.anchor, batch)?;
                crawled = crawled.saturating_add(1);
                eprintln!(
                    "INFO connected peer {}: persisted headers={} reached_limit={} header_request_error={}",
                    peer.addr, header_count, reached_limit, header_request_error,
                );
            }
            Err(error) => {
                let error = format_error_chain(&error);
                record_peer_failure(db, peer_id, &error)?;
                warn!(
                    session_id,
                    peer_id,
                    peer_addr = %peer.addr,
                    %error,
                    "connected peer diagnostic crawl failed"
                );
                eprintln!(
                    "WARN connected peer {} failed over RPC diagnostics: {error}",
                    peer.addr
                );
            }
        }
    }

    Ok(crawled)
}

async fn request_rpc_peer_headers(
    client: &RpcRequestClient,
    peer_addr: &str,
    known_blocks: Vec<block::Hash>,
    max_headers: usize,
) -> Result<HeaderBatch> {
    let mut collected = Vec::new();
    let initial_locator_len = known_blocks.len();
    let mut locator = known_blocks;
    let mut reached_limit = false;
    let mut request_round_trips = 0usize;
    let mut last_hash = None;

    while collected.len() < max_headers {
        request_round_trips = request_round_trips.saturating_add(1);
        let locator_strings: Vec<_> = locator.iter().map(ToString::to_string).collect();
        let params = json!([peer_addr, locator_strings, Value::Null]).to_string();

        let response: Value = match client
            .json_result_from_call("crosslink_getpeerheaders", params)
            .await
            .map_err(|error| eyre!(error))
        {
            Ok(response) => response,
            Err(error) => {
                return finish_header_request_after_error(collected, reached_limit, error)
            }
        };

        let header_hexes = match response.get("headers").and_then(Value::as_array) {
            Some(headers) => headers,
            None => {
                return finish_header_request_after_error(
                    collected,
                    reached_limit,
                    eyre!("crosslink_getpeerheaders response missing headers array"),
                )
            }
        };

        if header_hexes.is_empty() {
            info!(
                peer_addr,
                request_round_trips,
                headers_collected = collected.len(),
                "connected peer returned no additional headers through RPC diagnostics"
            );
            eprintln!(
                "INFO connected peer {peer_addr}: no additional headers after {} RPC diagnostic round trips collected={}",
                request_round_trips,
                collected.len(),
            );
            break;
        }

        let remaining = max_headers - collected.len();
        let header_count = header_hexes.len();
        let mut taken_headers = Vec::new();
        for header_hex in header_hexes.iter().take(remaining) {
            let Some(header_hex) = header_hex.as_str() else {
                return finish_header_request_after_error(
                    collected,
                    reached_limit,
                    eyre!("crosslink_getpeerheaders returned a non-string header"),
                );
            };
            taken_headers.push(counted_header_from_hex(header_hex)?);
        }

        reached_limit =
            header_count > remaining || collected.len() + taken_headers.len() >= max_headers;
        let batch_last_hash = taken_headers
            .last()
            .expect("taken headers is not empty")
            .header
            .hash();
        last_hash = Some(batch_last_hash);
        collected.extend(taken_headers);
        info!(
            peer_addr,
            request_round_trips,
            batch_header_count = header_count,
            headers_collected = collected.len(),
            max_headers,
            reached_limit,
            last_hash = %batch_last_hash,
            "received connected peer header batch through RPC diagnostics"
        );
        eprintln!(
            "INFO connected peer {peer_addr}: RPC diagnostic header batch round={} batch={} collected={}/{} reached_limit={} last_hash={}",
            request_round_trips,
            header_count,
            collected.len(),
            max_headers,
            reached_limit,
            batch_last_hash,
        );
        locator = vec![batch_last_hash];
    }

    info!(
        initial_locator_len,
        peer_addr,
        request_round_trips,
        headers_collected = collected.len(),
        reached_limit,
        last_hash = ?last_hash,
        "finished connected peer header requests through RPC diagnostics"
    );

    Ok(HeaderBatch {
        headers: collected,
        reached_limit,
        request_error: None,
    })
}

fn counted_header_from_hex(header_hex: &str) -> Result<block::CountedHeader> {
    let raw_header = hex::decode(header_hex.trim()).wrap_err("invalid raw header hex")?;
    let header =
        block::Header::zcash_deserialize(raw_header.as_slice()).wrap_err("invalid raw header")?;

    Ok(block::CountedHeader {
        header: Arc::new(header),
    })
}

async fn import_local_chain(
    db: &Connection,
    session_id: i64,
    client: &RpcRequestClient,
    rpc_addr: SocketAddr,
    genesis_hash: block::Hash,
    max_headers: usize,
) -> Result<()> {
    let peer_id = upsert_peer_label(db, &format!("local-rpc:{rpc_addr}"))?;
    let tip_height = local_block_count(client).await?;
    let tip_hash = local_block_hash(client, Height(tip_height)).await?;
    update_peer_success(db, peer_id, "local-rpc", "local-rpc", Height(tip_height))?;

    if stored_peer_tip_matches(db, peer_id, Height(tip_height), tip_hash)? {
        info!(
            session_id,
            peer_id,
            %rpc_addr,
            tip_height,
            tip_hash = %tip_hash,
            "local chain already indexed"
        );
        eprintln!(
            "INFO local chain import {rpc_addr}: already indexed tip={} hash={}",
            tip_height, tip_hash,
        );
        return Ok(());
    }

    let anchor = local_import_anchor(db, peer_id, client, genesis_hash, Height(tip_height)).await?;

    let remaining_heights = tip_height.saturating_sub(anchor.height.0);
    if remaining_heights == 0 {
        mark_peer_tip_observed(db, peer_id, anchor.height, anchor.hash)?;
        info!(
            session_id,
            peer_id,
            %rpc_addr,
            tip_height,
            tip_hash = %tip_hash,
            "local chain indexed tip recovered from best path"
        );
        eprintln!(
            "INFO local chain import {rpc_addr}: recovered indexed tip={} hash={} from best path",
            tip_height, tip_hash,
        );
        return Ok(());
    }

    let header_limit = usize::try_from(remaining_heights)
        .unwrap_or(usize::MAX)
        .min(max_headers);
    let mut headers = Vec::with_capacity(header_limit);
    info!(
        session_id,
        peer_id,
        %rpc_addr,
        tip_height,
        tip_hash = %tip_hash,
        anchor_height = anchor.height.0,
        anchor_hash = %anchor.hash,
        header_limit,
        "local chain import discovered tip"
    );
    eprintln!(
        "INFO local chain import {rpc_addr}: tip={} hash={} anchor={}@{} importing_up_to={} headers",
        tip_height, tip_hash, anchor.hash, anchor.height.0, header_limit,
    );

    for height in anchor.height.0.saturating_add(1)..=tip_height {
        if headers.len() >= max_headers {
            break;
        }

        headers.push(local_counted_header(client, Height(height)).await?);
        if height == tip_height
            || height % LOCAL_IMPORT_PROGRESS_INTERVAL == 0
            || headers.len() >= max_headers
        {
            info!(
                session_id,
                peer_id,
                %rpc_addr,
                height,
                tip_height,
                headers_collected = headers.len(),
                max_headers,
                "local chain import progress"
            );
            eprintln!(
                "INFO local chain import {rpc_addr}: height={height}/{tip_height} headers={}/{}",
                headers.len(),
                max_headers,
            );
        }
    }

    let header_count = headers.len();
    let reached_limit = usize::try_from(remaining_heights)
        .map(|remaining| remaining > header_count)
        .unwrap_or(false);
    persist_headers(
        db,
        session_id,
        peer_id,
        anchor,
        HeaderBatch {
            headers,
            reached_limit,
            request_error: None,
        },
    )?;

    info!(
        session_id,
        peer_id,
        %rpc_addr,
        tip_height,
        header_count,
        reached_limit,
        "imported local chain"
    );
    eprintln!(
        "INFO imported local chain from {rpc_addr}: headers={} tip_height={} reached_limit={}",
        header_count, tip_height, reached_limit,
    );

    Ok(())
}

fn stored_peer_tip_matches(
    db: &Connection,
    peer_id: i64,
    tip_height: Height,
    tip_hash: block::Hash,
) -> Result<bool> {
    let stored_tip = db
        .query_row(
            "SELECT tip_height, tip_hash FROM peer_tips WHERE peer_id = ?1",
            params![peer_id],
            |row| Ok((Height(row.get::<_, u32>(0)?), row.get::<_, String>(1)?)),
        )
        .optional()?;

    let Some((stored_height, stored_hash)) = stored_tip else {
        return Ok(false);
    };

    Ok(stored_height == tip_height && parse_hash(&stored_hash)? == tip_hash)
}

fn mark_peer_tip_observed(
    db: &Connection,
    peer_id: i64,
    tip_height: Height,
    tip_hash: block::Hash,
) -> Result<()> {
    db.execute(
        "INSERT INTO peer_tips(peer_id, tip_hash, tip_height, observed_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(peer_id) DO UPDATE SET
            tip_hash = excluded.tip_hash,
            tip_height = excluded.tip_height,
            observed_at = excluded.observed_at",
        params![peer_id, tip_hash.to_string(), tip_height.0, now_unix()?],
    )?;

    Ok(())
}

async fn local_import_anchor(
    db: &Connection,
    peer_id: i64,
    client: &RpcRequestClient,
    genesis_hash: block::Hash,
    tip_height: Height,
) -> Result<Anchor> {
    let stored_path: Vec<_> = {
        let mut statement = db.prepare(
            "SELECT height, hash FROM peer_best_path WHERE peer_id = ?1 AND height <= ?2 ORDER BY height DESC",
        )?;
        let rows = statement.query_map(params![peer_id, tip_height.0], |row| {
            Ok((Height(row.get::<_, u32>(0)?), row.get::<_, String>(1)?))
        })?;
        rows.collect::<std::result::Result<_, _>>()?
    };
    let highest_stored_height = stored_path.first().map(|(height, _)| height.0);

    for (height, stored_hash) in stored_path {
        let stored_hash = parse_hash(&stored_hash)?;
        if local_block_hash(client, height).await? == stored_hash {
            if highest_stored_height.is_some_and(|highest| highest > height.0) {
                db.execute("DELETE FROM peer_tips WHERE peer_id = ?1", params![peer_id])?;
                db.execute(
                    "DELETE FROM peer_best_path WHERE peer_id = ?1 AND height > ?2",
                    params![peer_id, height.0],
                )?;
            }

            return Ok(Anchor {
                height,
                hash: stored_hash,
            });
        }
    }

    Ok(Anchor {
        height: Height(0),
        hash: genesis_hash,
    })
}

fn persist_headers(
    db: &Connection,
    session_id: i64,
    peer_id: i64,
    fallback_anchor: Anchor,
    batch: HeaderBatch,
) -> Result<()> {
    let HeaderBatch {
        headers,
        reached_limit,
        request_error,
    } = batch;
    let header_count = headers.len();

    if headers.is_empty() {
        info!(session_id, peer_id, "no headers to persist");
        return Ok(());
    }

    info!(
        session_id,
        peer_id,
        header_count,
        fallback_anchor_height = fallback_anchor.height.0,
        fallback_anchor_hash = %fallback_anchor.hash,
        reached_limit,
        header_request_error = request_error.is_some(),
        "persisting peer headers"
    );
    eprintln!(
        "INFO persisting peer {peer_id} headers: count={} fallback_anchor={}@{} reached_limit={} header_request_error={}",
        header_count,
        fallback_anchor.hash,
        fallback_anchor.height.0,
        reached_limit,
        request_error.is_some(),
    );

    let now = now_unix()?;
    let mut current_anchor =
        find_anchor_for_parent(db, peer_id, headers[0].header.previous_block_hash)?
            .unwrap_or(fallback_anchor);

    if headers[0].header.previous_block_hash != current_anchor.hash {
        warn!(
            session_id,
            peer_id,
            expected_anchor_hash = %current_anchor.hash,
            first_parent_hash = %headers[0].header.previous_block_hash,
            "header anchor mismatch"
        );
        record_peer_event(
            db,
            Some(peer_id),
            "anchor_mismatch",
            json!({
                "expected_anchor_hash": current_anchor.hash.to_string(),
                "first_parent_hash": headers[0].header.previous_block_hash.to_string(),
            }),
        )?;
    }

    let mut last_tip = None;
    for counted_header in headers {
        let parent_hash = counted_header.header.previous_block_hash;
        if parent_hash != current_anchor.hash {
            let Some(recovered_anchor) = find_anchor_for_parent(db, peer_id, parent_hash)? else {
                warn!(session_id, peer_id, %parent_hash, "reset or unknown branch while persisting headers");
                record_peer_event(
                    db,
                    Some(peer_id),
                    "reset_or_unknown_branch",
                    json!({ "parent_hash": parent_hash.to_string() }),
                )?;
                break;
            };
            info!(
                session_id,
                peer_id,
                parent_hash = %parent_hash,
                recovered_height = recovered_anchor.height.0,
                recovered_hash = %recovered_anchor.hash,
                "switched persistence anchor to known branch"
            );
            eprintln!(
                "INFO peer {peer_id}: switched persistence anchor to {}@{} for parent {}",
                recovered_anchor.hash, recovered_anchor.height.0, parent_hash,
            );
            current_anchor = recovered_anchor;
        }

        let height = next_height(current_anchor.height)?;
        let hash = counted_header.header.hash();
        let raw_header_hex = hex::encode(counted_header.header.zcash_serialize_to_vec()?);

        db.execute(
            "INSERT INTO headers(hash, parent_hash, height, raw_header_hex, first_seen, last_seen)
             VALUES (?1, ?2, ?3, ?4, ?5, ?5)
             ON CONFLICT(hash) DO UPDATE SET
                parent_hash = excluded.parent_hash,
                height = COALESCE(headers.height, excluded.height),
                raw_header_hex = excluded.raw_header_hex,
                last_seen = excluded.last_seen",
            params![
                hash.to_string(),
                parent_hash.to_string(),
                height.0,
                raw_header_hex,
                now,
            ],
        )?;

        db.execute(
            "INSERT INTO peer_header_observations(peer_id, height, hash, crawl_session_id, observed_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(peer_id, height, hash, crawl_session_id) DO UPDATE SET
                observed_at = excluded.observed_at",
            params![peer_id, height.0, hash.to_string(), session_id, now],
        )?;

        db.execute(
            "INSERT INTO peer_best_path(peer_id, height, hash, updated_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(peer_id, height) DO UPDATE SET
                hash = excluded.hash,
                updated_at = excluded.updated_at",
            params![peer_id, height.0, hash.to_string(), now],
        )?;

        current_anchor = Anchor { height, hash };
        last_tip = Some(PeerTip { height, hash });
    }

    if let Some(tip) = last_tip {
        db.execute(
            "DELETE FROM peer_best_path WHERE peer_id = ?1 AND height > ?2",
            params![peer_id, tip.height.0],
        )?;

        if reached_limit || request_error.is_some() {
            db.execute("DELETE FROM peer_tips WHERE peer_id = ?1", params![peer_id])?;

            if reached_limit {
                info!(
                    session_id,
                    peer_id,
                    height = tip.height.0,
                    hash = %tip.hash,
                    "header persistence reached configured limit"
                );

                record_peer_event(
                    db,
                    Some(peer_id),
                    "header_limit_reached",
                    json!({
                        "height": tip.height.0,
                        "hash": tip.hash.to_string(),
                    }),
                )?;
            }

            if let Some(error) = request_error.as_deref() {
                warn!(
                    session_id,
                    peer_id,
                    height = tip.height.0,
                    hash = %tip.hash,
                    %error,
                    "persisted partial headers after request failure"
                );

                record_peer_event(
                    db,
                    Some(peer_id),
                    "header_request_error",
                    json!({
                        "height": tip.height.0,
                        "hash": tip.hash.to_string(),
                        "error": error,
                    }),
                )?;
            }
        } else {
            info!(
                session_id,
                peer_id,
                tip_height = tip.height.0,
                tip_hash = %tip.hash,
                "observed peer tip"
            );
            db.execute(
                "INSERT INTO peer_tips(peer_id, tip_hash, tip_height, observed_at)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(peer_id) DO UPDATE SET
                    tip_hash = excluded.tip_hash,
                    tip_height = excluded.tip_height,
                    observed_at = excluded.observed_at",
                params![peer_id, tip.hash.to_string(), tip.height.0, now],
            )?;
        }

        info!(
            session_id,
            peer_id,
            persisted_header_count = header_count,
            final_height = tip.height.0,
            final_hash = %tip.hash,
            reached_limit,
            header_request_error = request_error.is_some(),
            "persisted peer headers"
        );
        eprintln!(
            "INFO persisted peer {peer_id} headers: count={} final_height={} reached_limit={} header_request_error={}",
            header_count,
            tip.height.0,
            reached_limit,
            request_error.is_some(),
        );
    }

    Ok(())
}

fn find_anchor_for_parent(
    db: &Connection,
    peer_id: i64,
    parent_hash: block::Hash,
) -> Result<Option<Anchor>> {
    let anchor = db
        .query_row(
            "SELECT height, hash FROM peer_best_path WHERE peer_id = ?1 AND hash = ?2 LIMIT 1",
            params![peer_id, parent_hash.to_string()],
            |row| {
                let height: u32 = row.get(0)?;
                let hash: String = row.get(1)?;
                Ok((Height(height), hash))
            },
        )
        .optional()?;

    anchor
        .map(|(height, hash)| parse_hash(&hash).map(|hash| Anchor { height, hash }))
        .transpose()
}

fn ensure_genesis_header(db: &Connection, genesis_hash: block::Hash) -> Result<()> {
    let now = now_unix()?;
    db.execute(
        "INSERT INTO headers(hash, parent_hash, height, raw_header_hex, first_seen, last_seen)
         VALUES (?1, ?1, 0, '', ?2, ?2)
         ON CONFLICT(hash) DO NOTHING",
        params![genesis_hash.to_string(), now],
    )?;
    Ok(())
}

async fn connect_peer(
    network: &Network,
    peer_addr: SocketAddr,
    user_agent: String,
    connect_timeout_secs: u64,
) -> Result<zebra_network::Client> {
    let connect_timeout = Duration::from_secs(connect_timeout_secs);
    ensure!(
        connect_timeout_secs > 0,
        "connect timeout must be greater than 0"
    );

    timeout(
        connect_timeout,
        connect_isolated_tcp_direct(network, peer_addr, user_agent),
    )
    .await
    .map_err(|_elapsed| eyre!("timed out connecting to remote peer {peer_addr}"))?
    .map_err(|error| eyre!(error.to_string()))
    .wrap_err_with(|| format!("failed to connect to remote peer {peer_addr}"))
}

async fn local_rpc_peers(client: &RpcRequestClient) -> Result<Vec<SocketAddr>> {
    let peers: Value = client
        .json_result_from_call("getpeerinfo", "[]")
        .await
        .map_err(|error| eyre!(error))?;

    let Some(peers) = peers.as_array() else {
        return Ok(Vec::new());
    };

    Ok(peers
        .iter()
        .filter_map(|peer| peer.get("addr"))
        .filter_map(Value::as_str)
        .filter_map(|addr| addr.parse().ok())
        .collect())
}

async fn local_rpc_connected_peers(client: &RpcRequestClient) -> Result<Vec<RpcConnectedPeer>> {
    let peers: Value = client
        .json_result_from_call("crosslink_getconnectedpeers", "[]")
        .await
        .map_err(|error| eyre!(error))?;

    let Some(peers) = peers.as_array() else {
        return Ok(Vec::new());
    };

    peers
        .iter()
        .map(|peer| {
            let addr = peer
                .get("addr")
                .and_then(Value::as_str)
                .ok_or_else(|| eyre!("connected peer missing addr"))?
                .to_string();
            let socket_addr = addr.parse().ok();
            let inbound = peer
                .get("inbound")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let ready = peer.get("ready").and_then(Value::as_bool).unwrap_or(false);
            let user_agent = peer
                .get("user_agent")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let services = peer
                .get("services")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let advertised_height = peer
                .get("advertised_height")
                .and_then(Value::as_u64)
                .and_then(|height| u32::try_from(height).ok())
                .map(Height)
                .unwrap_or(Height(0));

            Ok(RpcConnectedPeer {
                addr,
                socket_addr,
                inbound,
                ready,
                user_agent,
                services,
                advertised_height,
            })
        })
        .collect()
}

fn stored_peer_addrs(db: &Connection) -> Result<Vec<SocketAddr>> {
    let mut statement =
        db.prepare("SELECT addr FROM peers WHERE status != 'failed' ORDER BY RANDOM()")?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;

    Ok(rows
        .filter_map(|row| row.ok())
        .filter_map(|addr| addr.parse().ok())
        .collect())
}

fn is_safe_remote_discovered_addr(addr: SocketAddr) -> bool {
    let ip = addr.ip();

    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_multicast()
        || match ip {
            std::net::IpAddr::V4(ip) => {
                ip.is_private()
                    || ip.is_link_local()
                    || ip.is_broadcast()
                    || is_ipv4_documentation(ip)
            }
            std::net::IpAddr::V6(ip) => {
                ip.is_unique_local()
                    || ip.is_unicast_link_local()
                    || is_ipv6_documentation(ip)
                    || is_ipv6_deprecated_site_local(ip)
            }
        })
}

fn is_ipv4_documentation(ip: std::net::Ipv4Addr) -> bool {
    let octets = ip.octets();
    matches!(
        octets,
        [192, 0, 2, _] | [198, 51, 100, _] | [203, 0, 113, _]
    )
}

fn is_ipv6_documentation(ip: std::net::Ipv6Addr) -> bool {
    let segments = ip.segments();
    segments[0] == 0x2001 && segments[1] == 0x0db8
}

fn is_ipv6_deprecated_site_local(ip: std::net::Ipv6Addr) -> bool {
    ip.segments()[0] & 0xffc0 == 0xfec0
}

async fn local_block_hash(client: &RpcRequestClient, height: Height) -> Result<block::Hash> {
    let hash: String = client
        .json_result_from_call("getblockhash", format!("[{}]", height.0))
        .await
        .map_err(|error| eyre!(error))?;

    parse_hash(&hash)
}

async fn local_block_count(client: &RpcRequestClient) -> Result<u32> {
    client
        .json_result_from_call("getblockcount", "[]")
        .await
        .map_err(|error| eyre!(error))
}

async fn local_counted_header(
    client: &RpcRequestClient,
    height: Height,
) -> Result<block::CountedHeader> {
    let hash = local_block_hash(client, height).await?;
    let raw_header_hex: String = client
        .json_result_from_call("getblockheader", format!(r#"["{}", false]"#, hash))
        .await
        .map_err(|error| eyre!(error))?;
    let raw_header = hex::decode(raw_header_hex.trim())
        .wrap_err_with(|| format!("invalid raw header hex at height {}", height.0))?;
    let header = block::Header::zcash_deserialize(raw_header.as_slice())
        .wrap_err_with(|| format!("invalid raw header at height {}", height.0))?;

    Ok(block::CountedHeader {
        header: Arc::new(header),
    })
}

fn enqueue_peer(
    db: &Connection,
    queue: &mut VecDeque<SocketAddr>,
    queued: &mut HashSet<SocketAddr>,
    source_peer_id: Option<i64>,
    peer_addr: SocketAddr,
) -> Result<()> {
    upsert_peer_seen(db, peer_addr)?;
    if let Some(source_peer_id) = source_peer_id {
        record_discovered_addr(db, source_peer_id, peer_addr)?;
    }
    if queued.insert(peer_addr) {
        queue.push_back(peer_addr);
    }
    Ok(())
}

fn upsert_peer_seen(db: &Connection, peer_addr: SocketAddr) -> Result<i64> {
    upsert_peer_label(db, &peer_addr.to_string())
}

fn upsert_peer_label(db: &Connection, peer_addr: &str) -> Result<i64> {
    let now = now_unix()?;
    db.execute(
        "INSERT INTO peers(addr, first_seen, last_seen)
         VALUES (?1, ?2, ?2)
         ON CONFLICT(addr) DO UPDATE SET last_seen = excluded.last_seen",
        params![peer_addr, now],
    )?;

    Ok(db.query_row(
        "SELECT id FROM peers WHERE addr = ?1",
        params![peer_addr],
        |row| row.get(0),
    )?)
}

fn update_peer_success(
    db: &Connection,
    peer_id: i64,
    user_agent: &str,
    services: &str,
    advertised_height: Height,
) -> Result<()> {
    let now = now_unix()?;
    db.execute(
        "UPDATE peers SET
            last_seen = ?2,
            last_success = ?2,
            user_agent = ?3,
            services = ?4,
            advertised_height = ?5,
            status = 'live'
         WHERE id = ?1",
        params![peer_id, now, user_agent, services, advertised_height.0],
    )?;
    Ok(())
}

fn record_peer_failure(db: &Connection, peer_id: i64, error: &str) -> Result<()> {
    let now = now_unix()?;
    db.execute(
        "UPDATE peers SET last_seen = ?2, last_failure = ?2, status = 'failed' WHERE id = ?1",
        params![peer_id, now],
    )?;
    record_peer_event(db, Some(peer_id), "crawl_error", json!({ "error": error }))
}

fn record_discovered_addr(
    db: &Connection,
    source_peer_id: i64,
    peer_addr: SocketAddr,
) -> Result<()> {
    let now = now_unix()?;
    db.execute(
        "INSERT INTO peer_addresses(source_peer_id, addr, first_seen, last_seen)
         VALUES (?1, ?2, ?3, ?3)
         ON CONFLICT(source_peer_id, addr) DO UPDATE SET last_seen = excluded.last_seen",
        params![source_peer_id, peer_addr.to_string(), now],
    )?;
    Ok(())
}

fn record_peer_event(
    db: &Connection,
    peer_id: Option<i64>,
    kind: &str,
    details: Value,
) -> Result<()> {
    db.execute(
        "INSERT INTO peer_events(peer_id, kind, details_json, observed_at) VALUES (?1, ?2, ?3, ?4)",
        params![peer_id, kind, details.to_string(), now_unix()?],
    )?;
    Ok(())
}

fn start_crawl_session(db: &Connection) -> Result<i64> {
    db.execute(
        "INSERT INTO crawl_sessions(started_at) VALUES (?1)",
        params![now_unix()?],
    )?;
    Ok(db.last_insert_rowid())
}

fn finish_crawl_session(
    db: &Connection,
    session_id: i64,
    peer_count: usize,
    notes: &str,
) -> Result<()> {
    db.execute(
        "UPDATE crawl_sessions SET finished_at = ?2, peer_count = ?3, notes = ?4 WHERE id = ?1",
        params![session_id, now_unix()?, peer_count, notes],
    )?;
    Ok(())
}

fn export(db: &Connection, args: ExportArgs) -> Result<()> {
    match args.format {
        ExportFormat::Json => println!("{}", export_json(db)?),
        ExportFormat::Dot => println!("{}", export_dot(db, &args)?),
        ExportFormat::Html => println!("{}", export_html(db, &args)?),
    }
    Ok(())
}

fn export_json(db: &Connection) -> Result<Value> {
    let peers = query_json_array(
        db,
        "SELECT id, addr, status, user_agent, advertised_height, last_success, last_failure FROM peers ORDER BY addr",
        |row| {
            Ok(json!({
                "id": row.get::<_, i64>(0)?,
                "addr": row.get::<_, String>(1)?,
                "status": row.get::<_, String>(2)?,
                "user_agent": row.get::<_, Option<String>>(3)?,
                "advertised_height": row.get::<_, Option<u32>>(4)?,
                "last_success": row.get::<_, Option<i64>>(5)?,
                "last_failure": row.get::<_, Option<i64>>(6)?,
            }))
        },
    )?;

    let headers = query_json_array(
        db,
        "SELECT
            h.hash,
            h.parent_hash,
            h.height,
            COUNT(DISTINCT b.peer_id) AS current_peer_count,
            COUNT(DISTINCT o.peer_id) AS observed_peer_count
         FROM headers h
         LEFT JOIN peer_header_observations o ON o.hash = h.hash
         LEFT JOIN peer_best_path b ON b.hash = h.hash
         GROUP BY h.hash, h.parent_hash, h.height
         ORDER BY h.height, h.hash",
        |row| {
            Ok(json!({
                "hash": row.get::<_, String>(0)?,
                "parent_hash": row.get::<_, String>(1)?,
                "height": row.get::<_, Option<u32>>(2)?,
                "current_peer_count": row.get::<_, i64>(3)?,
                "observed_peer_count": row.get::<_, i64>(4)?,
            }))
        },
    )?;

    let tips = query_json_array(
        db,
        "SELECT p.addr, t.tip_height, t.tip_hash, t.observed_at
         FROM peer_tips t JOIN peers p ON p.id = t.peer_id
         ORDER BY t.tip_height DESC, p.addr",
        |row| {
            Ok(json!({
                "peer_addr": row.get::<_, String>(0)?,
                "tip_height": row.get::<_, u32>(1)?,
                "tip_hash": row.get::<_, String>(2)?,
                "observed_at": row.get::<_, i64>(3)?,
            }))
        },
    )?;

    Ok(json!({
        "peers": peers,
        "headers": headers,
        "peer_tips": tips,
    }))
}

fn query_json_array<F>(db: &Connection, sql: &str, mut map: F) -> Result<Vec<Value>>
where
    F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<Value>,
{
    let mut statement = db.prepare(sql)?;
    let rows = statement.query_map([], |row| map(row))?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn export_dot(db: &Connection, args: &ExportArgs) -> Result<String> {
    ensure!(
        args.dot_sample_interval > 0,
        "--dot-sample-interval must be greater than 0"
    );

    let headers = dot_headers(db)?;
    let included = included_dot_headers(&headers, args);
    let by_hash = headers
        .iter()
        .map(|header| (header.hash.clone(), header))
        .collect::<HashMap<_, _>>();

    let mut dot = String::from(
        "digraph forks {\n  rankdir=LR;\n  graph [nodesep=0.2,ranksep=0.35];\n  node [shape=box,fontname=monospace,fontsize=10,width=0.1,height=0.1,margin=0.04];\n  edge [fontsize=9,arrowsize=0.6];\n",
    );

    if !args.dot_full {
        writeln!(
            dot,
            "  // Compact DOT export: forks, tips, genesis, and every {} heights. Use --dot-full to include every header.",
            args.dot_sample_interval
        )?;
    }

    for header in &headers {
        if !included.contains(&header.hash) {
            continue;
        }

        let short_hash = short_hash(&header.hash);
        let label_height = header
            .height
            .map(|height| height.to_string())
            .unwrap_or_else(|| "?".to_string());
        let mut annotations = Vec::new();
        if header.child_count > 1 {
            annotations.push(format!("forks={}", header.child_count));
        }
        if header.is_tip {
            annotations.push("tip".to_string());
        }
        if header.current_peer_count > 0 || header.observed_peer_count > 0 {
            annotations.push(format!(
                "cur={} obs={}",
                header.current_peer_count, header.observed_peer_count
            ));
        }
        let annotations = if annotations.is_empty() {
            String::new()
        } else {
            format!("\\n{}", annotations.join(" "))
        };

        writeln!(
            dot,
            "  \"{}\" [label=\"h={}\\n{}{}\"] ;",
            header.hash, label_height, short_hash, annotations,
        )?;
    }

    for header in &headers {
        if !included.contains(&header.hash) || header.parent_hash == header.hash {
            continue;
        }

        if let Some((ancestor_hash, skipped)) =
            nearest_included_ancestor(header, &included, &by_hash)
        {
            if skipped == 0 {
                writeln!(dot, "  \"{}\" -> \"{}\";", ancestor_hash, header.hash)?;
            } else {
                writeln!(
                    dot,
                    "  \"{}\" -> \"{}\" [label=\"+{}\"] ;",
                    ancestor_hash, header.hash, skipped
                )?;
            }
        }
    }

    dot.push_str("}\n");
    Ok(dot)
}

fn export_html(db: &Connection, args: &ExportArgs) -> Result<String> {
    let graph_json = serde_json::to_string(&export_browser_graph(db, args)?)?;
    Ok(HTML_VIEWER_TEMPLATE.replace("__FORK_TRACKER_GRAPH__", &graph_json))
}

fn export_browser_graph(db: &Connection, args: &ExportArgs) -> Result<Value> {
    ensure!(
        args.dot_sample_interval > 0,
        "--dot-sample-interval must be greater than 0"
    );

    let headers = dot_headers(db)?;
    let included = included_dot_headers(&headers, args);
    let by_hash = headers
        .iter()
        .map(|header| (header.hash.clone(), header))
        .collect::<HashMap<_, _>>();

    let nodes = headers
        .iter()
        .filter(|header| included.contains(&header.hash))
        .map(|header| {
            json!({
                "id": header.hash,
                "parent_hash": header.parent_hash,
                "height": header.height,
                "short_hash": short_hash(&header.hash),
                "current_peer_count": header.current_peer_count,
                "observed_peer_count": header.observed_peer_count,
                "child_count": header.child_count,
                "is_tip": header.is_tip,
            })
        })
        .collect::<Vec<_>>();

    let edges = headers
        .iter()
        .filter(|header| included.contains(&header.hash) && header.parent_hash != header.hash)
        .filter_map(|header| {
            nearest_included_ancestor(header, &included, &by_hash).map(
                |(ancestor_hash, skipped)| {
                    json!({
                        "source": ancestor_hash,
                        "target": header.hash,
                        "skipped": skipped,
                    })
                },
            )
        })
        .collect::<Vec<_>>();

    Ok(json!({
        "compact": !args.dot_full,
        "sample_interval": args.dot_sample_interval,
        "nodes": nodes,
        "edges": edges,
    }))
}

fn dot_headers(db: &Connection) -> Result<Vec<DotHeader>> {
    let mut statement = db.prepare(
        "SELECT
            h.hash,
            h.parent_hash,
            h.height,
            COUNT(DISTINCT b.peer_id) AS current_peer_count,
            COUNT(DISTINCT o.peer_id) AS observed_peer_count,
            (SELECT COUNT(*) FROM headers child WHERE child.parent_hash = h.hash AND child.hash != h.hash) AS child_count,
            EXISTS(SELECT 1 FROM peer_tips tip WHERE tip.tip_hash = h.hash) AS is_tip
         FROM headers h
         LEFT JOIN peer_header_observations o ON o.hash = h.hash
         LEFT JOIN peer_best_path b ON b.hash = h.hash
         GROUP BY h.hash, h.parent_hash, h.height
         ORDER BY h.height, h.hash",
    )?;
    let rows = statement.query_map([], |row| {
        Ok(DotHeader {
            hash: row.get::<_, String>(0)?,
            parent_hash: row.get::<_, String>(1)?,
            height: row.get::<_, Option<u32>>(2)?,
            current_peer_count: row.get::<_, i64>(3)?,
            observed_peer_count: row.get::<_, i64>(4)?,
            child_count: row.get::<_, i64>(5)?,
            is_tip: row.get::<_, bool>(6)?,
        })
    })?;

    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn included_dot_headers(headers: &[DotHeader], args: &ExportArgs) -> HashSet<String> {
    let fork_parent_hashes = headers
        .iter()
        .filter(|header| header.child_count > 1)
        .map(|header| header.hash.as_str())
        .collect::<HashSet<_>>();

    headers
        .iter()
        .filter(|header| {
            args.dot_full
                || header.height == Some(0)
                || header.child_count != 1
                || fork_parent_hashes.contains(header.parent_hash.as_str())
                || header.is_tip
                || header
                    .height
                    .is_some_and(|height| height % args.dot_sample_interval == 0)
        })
        .map(|header| header.hash.clone())
        .collect()
}

fn nearest_included_ancestor(
    header: &DotHeader,
    included: &HashSet<String>,
    by_hash: &HashMap<String, &DotHeader>,
) -> Option<(String, u32)> {
    let mut ancestor_hash = header.parent_hash.clone();
    let mut skipped = 0;

    while let Some(ancestor) = by_hash.get(&ancestor_hash) {
        if included.contains(&ancestor.hash) {
            return Some((ancestor.hash.clone(), skipped));
        }

        skipped += 1;
        if ancestor.parent_hash == ancestor.hash {
            return None;
        }
        ancestor_hash.clone_from(&ancestor.parent_hash);
    }

    None
}

fn short_hash(hash: &str) -> String {
    hash.chars().take(12).collect()
}

const HTML_VIEWER_TEMPLATE: &str = r##"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Crosslink Fork Tracker</title>
<style>
:root { color-scheme: dark; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
body { margin: 0; background: #111827; color: #e5e7eb; }
#toolbar { position: sticky; top: 0; z-index: 2; display: flex; gap: 0.75rem; align-items: center; padding: 0.75rem 1rem; background: #030712; border-bottom: 1px solid #374151; }
#summary { color: #9ca3af; white-space: nowrap; }
#details-panel { position: fixed; top: 4.25rem; left: 50%; z-index: 3; display: none; width: min(46rem, calc(100vw - 2rem)); max-height: min(22rem, calc(100vh - 6rem)); transform: translateX(-50%); background: #030712f2; border: 1px solid #374151; border-radius: 0.65rem; box-shadow: 0 1rem 2rem #000b; backdrop-filter: blur(8px); }
#details-panel.open { display: block; }
#details-header { display: flex; justify-content: space-between; align-items: center; gap: 1rem; padding: 0.6rem 0.75rem; border-bottom: 1px solid #374151; color: #e5e7eb; }
#details-close { border: 1px solid #4b5563; border-radius: 999px; padding: 0.15rem 0.55rem; color: #e5e7eb; background: #111827; cursor: pointer; font: inherit; }
#details-close:hover { border-color: #facc15; color: #facc15; }
#details { max-height: calc(min(22rem, calc(100vh - 6rem)) - 3rem); margin: 0; padding: 0.75rem; overflow: auto; white-space: pre-wrap; }
#viewport { width: 100vw; height: calc(100vh - 3.3rem); overflow: auto; cursor: grab; }
#viewport.dragging { cursor: grabbing; }
svg { display: block; background: radial-gradient(circle at top left, #1f2937, #111827 45rem); }
.edge { fill: none; stroke: #6b7280; stroke-width: 2; }
.edge.branch { stroke: #f59e0b; }
.edge-label { fill: #9ca3af; font-size: 12px; }
.node rect { fill: #1f2937; stroke: #60a5fa; stroke-width: 2; rx: 6; }
.node.tip rect { stroke: #34d399; }
.node.fork rect { stroke: #f59e0b; }
.node.selected rect { fill: #172554; stroke: #facc15; stroke-width: 3; }
.node text { fill: #e5e7eb; font-size: 12px; pointer-events: none; }
.muted { color: #9ca3af; }
</style>
</head>
<body>
<div id="toolbar">
  <strong>Crosslink Fork Tracker</strong>
  <span id="summary"></span>
</div>
<div id="viewport"><svg id="graph" xmlns="http://www.w3.org/2000/svg"></svg></div>
<section id="details-panel" aria-live="polite" aria-label="Node details">
  <div id="details-header"><strong>Node details</strong><button id="details-close" type="button" aria-label="Close node details">Close</button></div>
  <pre id="details" class="muted"></pre>
</section>
<script>
const graph = __FORK_TRACKER_GRAPH__;
const svg = document.getElementById('graph');
const viewport = document.getElementById('viewport');
const detailsPanel = document.getElementById('details-panel');
const details = document.getElementById('details');
const detailsClose = document.getElementById('details-close');
const nodes = graph.nodes.map((node, index) => ({ ...node, index }));
const byId = new Map(nodes.map(node => [node.id, node]));
const children = new Map();
const incoming = new Map();
for (const edge of graph.edges) {
  if (!children.has(edge.source)) children.set(edge.source, []);
  children.get(edge.source).push(edge.target);
  incoming.set(edge.target, edge.source);
}
for (const childList of children.values()) {
  childList.sort((a, b) => compareChildNodes(byId.get(a), byId.get(b)));
}
nodes.sort(compareNodes);

const heightRanks = new Map([...new Set(nodes.map(node => node.height ?? node.index))]
  .sort((a, b) => a - b)
  .map((height, index) => [height, index]));
const maxHeightRank = Math.max(0, ...heightRanks.values());
let nextBranchLane = 1;
const usedChildLanes = new Map();
for (const node of nodes) {
  const renderedParent = byId.get(incoming.get(node.id));
  if (renderedParent && Number.isFinite(renderedParent.lane)) {
    const siblings = children.get(renderedParent.id) ?? [];
    const siblingIndex = siblings.indexOf(node.id);
    const used = usedChildLanes.get(renderedParent.id) ?? 0;
    if (siblingIndex === 0 && used === 0) {
      node.lane = renderedParent.lane;
    } else {
      node.lane = alternatingBranchLane(nextBranchLane++);
    }
    usedChildLanes.set(renderedParent.id, used + 1);
  } else if (!Number.isFinite(node.lane)) {
    node.lane = 0;
  }
  const heightRank = heightRanks.get(node.height ?? node.index) ?? node.index;
  node.y = 96 + (maxHeightRank - heightRank) * 120;
}

const minLane = Math.min(0, ...nodes.map(node => node.lane));
for (const node of nodes) {
  node.x = 96 + (node.lane - minLane) * 170;
}

const nodeWidth = 120;
const nodeHeight = 64;
const width = Math.max(900, ...nodes.map(node => node.x + nodeWidth + 30));
const height = Math.max(500, ...nodes.map(node => node.y + nodeHeight + 48));
svg.setAttribute('width', width);
svg.setAttribute('height', height);
svg.setAttribute('viewBox', `0 0 ${width} ${height}`);
document.getElementById('summary').textContent = `${nodes.length} nodes, ${graph.edges.length} edges${graph.compact ? `, compact every ${graph.sample_interval} heights` : ', full graph'}`;
const trunkX = 96 + (0 - minLane) * 170;
viewport.scrollLeft = Math.max(0, trunkX - Math.min(180, viewport.clientWidth / 3));

for (const edge of graph.edges) {
  const source = byId.get(edge.source);
  const target = byId.get(edge.target);
  if (!source || !target) continue;
  const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
  const startX = source.x + nodeWidth / 2;
  const startY = source.y;
  const endX = target.x + nodeWidth / 2;
  const endY = target.y + nodeHeight;
  const midY = endY + Math.max(34, (startY - endY) / 2);
  path.setAttribute('class', `edge${source.lane === target.lane ? '' : ' branch'}`);
  path.setAttribute('d', `M ${startX} ${startY} L ${startX} ${midY} L ${endX} ${midY} L ${endX} ${endY}`);
  svg.appendChild(path);
  if (edge.skipped > 0) {
    const label = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    label.setAttribute('class', 'edge-label');
    label.setAttribute('x', source.lane === target.lane ? startX + 8 : (startX + endX) / 2 + 8);
    label.setAttribute('y', midY - 6);
    label.textContent = `+${edge.skipped}`;
    svg.appendChild(label);
  }
}

for (const node of nodes) {
  const group = document.createElementNS('http://www.w3.org/2000/svg', 'g');
  group.setAttribute('class', `node${node.is_tip ? ' tip' : ''}${node.child_count > 1 ? ' fork' : ''}`);
  group.setAttribute('transform', `translate(${node.x},${node.y})`);
  group.dataset.id = node.id;
  group.innerHTML = `<rect width="${nodeWidth}" height="${nodeHeight}"></rect><text x="8" y="18">h=${node.height ?? '?'}</text><text x="8" y="36">obs=${node.observed_peer_count ?? 0}</text><text x="8" y="54">${node.short_hash}</text>`;
  group.addEventListener('click', () => selectNode(node.id));
  svg.appendChild(group);
}

function compareNodes(a, b) {
  return ((a?.height ?? Number.MAX_SAFE_INTEGER) - (b?.height ?? Number.MAX_SAFE_INTEGER)) || String(a?.id).localeCompare(String(b?.id));
}

function compareChildNodes(a, b) {
  return ((b?.current_peer_count ?? 0) - (a?.current_peer_count ?? 0))
    || ((b?.observed_peer_count ?? 0) - (a?.observed_peer_count ?? 0))
    || Number(Boolean(b?.is_tip)) - Number(Boolean(a?.is_tip))
    || compareNodes(a, b);
}

function alternatingBranchLane(index) {
  const distance = Math.ceil(index / 2);
  return index % 2 === 1 ? -distance : distance;
}

function selectNode(id) {
  const node = byId.get(id);
  if (!node) return;
  document.querySelectorAll('.node.selected').forEach(element => element.classList.remove('selected'));
  const element = document.querySelector(`.node[data-id="${CSS.escape(id)}"]`);
  element?.classList.add('selected');
  element?.scrollIntoView({ block: 'center', inline: 'center' });
  details.textContent = JSON.stringify(node, null, 2);
  detailsPanel.classList.add('open');
}

detailsClose.addEventListener('click', () => {
  detailsPanel.classList.remove('open');
  document.querySelectorAll('.node.selected').forEach(element => element.classList.remove('selected'));
});

let dragging = false;
let dragX = 0;
let dragY = 0;
viewport.addEventListener('mousedown', event => { dragging = true; dragX = event.clientX; dragY = event.clientY; viewport.classList.add('dragging'); });
window.addEventListener('mouseup', () => { dragging = false; viewport.classList.remove('dragging'); });
window.addEventListener('mousemove', event => {
  if (!dragging) return;
  viewport.scrollLeft -= event.clientX - dragX;
  viewport.scrollTop -= event.clientY - dragY;
  dragX = event.clientX;
  dragY = event.clientY;
});
</script>
</body>
</html>"##;

const LOCAL_IMPORT_PROGRESS_INTERVAL: u32 = 10_000;

fn next_height(height: Height) -> Result<Height> {
    let next_height = height
        .0
        .checked_add(1)
        .ok_or_else(|| eyre!("height {} cannot be incremented", height.0))?;
    Ok(Height(next_height))
}

fn parse_hash(hash: &str) -> Result<block::Hash> {
    block::Hash::from_str(hash).wrap_err_with(|| format!("invalid block hash {hash}"))
}

fn format_error_chain(error: &Report) -> String {
    let mut formatted = String::new();

    for (index, cause) in error.chain().enumerate() {
        if index > 0 {
            formatted.push_str(": ");
        }
        formatted.push_str(&cause.to_string());
    }

    formatted
}

fn now_unix() -> Result<i64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .wrap_err("system clock is before UNIX_EPOCH")?;
    i64::try_from(now.as_secs()).wrap_err("current UNIX timestamp does not fit in i64")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_crosslink_network_profiles() {
        assert_eq!(
            NetworkArg::from_str("crosslink").unwrap(),
            NetworkArg::CrosslinkTestnet0,
        );
        assert_eq!(
            NetworkArg::from_str("clt0").unwrap(),
            NetworkArg::CrosslinkTestnet0,
        );
        assert_eq!(
            NetworkArg::from_str("cltn").unwrap(),
            NetworkArg::CrosslinkTestnet,
        );
        assert_eq!(
            NetworkArg::from_str("clrn").unwrap(),
            NetworkArg::CrosslinkRegtestnet,
        );
    }

    #[test]
    fn short_hash_uses_first_twelve_chars() {
        assert_eq!(short_hash("0123456789abcdef"), "0123456789ab");
    }

    #[test]
    fn parses_html_export_format() {
        assert_eq!(ExportFormat::from_str("html").unwrap(), ExportFormat::Html);
    }

    #[test]
    fn formats_error_chain_without_backtrace() {
        let error = eyre!("bytes remaining on stream").wrap_err("Serialization error");

        assert_eq!(
            format_error_chain(&error),
            "Serialization error: bytes remaining on stream"
        );
    }

    #[test]
    fn parses_daemon_command_with_crawl_args() {
        let args = Args::from_iter_safe([
            "fork-tracker",
            "daemon",
            "--peer",
            "127.0.0.1:8233",
            "--max-peers",
            "7",
            "--concurrency",
            "2",
            "--rounds",
            "2",
            "--crawl-interval-secs",
            "30",
            "--max-iterations",
            "3",
        ])
        .unwrap();

        let Command::Daemon(daemon_args) = args.command else {
            panic!("expected daemon command");
        };

        assert_eq!(
            daemon_args.crawl.peer,
            vec!["127.0.0.1:8233".parse().unwrap()]
        );
        assert_eq!(daemon_args.crawl.max_peers, 7);
        assert_eq!(daemon_args.crawl.concurrency, 2);
        assert_eq!(daemon_args.crawl.rounds, 2);
        assert_eq!(daemon_args.crawl_interval_secs, 30);
        assert_eq!(daemon_args.max_iterations, Some(3));
    }

    #[test]
    fn daemon_interval_must_be_positive() {
        let args = DaemonArgs {
            crawl: default_crawl_args(),
            crawl_interval_secs: 0,
            max_iterations: None,
        };

        assert!(validate_daemon_args(&args).is_err());
    }

    #[test]
    fn compact_dot_keeps_genesis_samples_forks_and_tips() {
        let args = ExportArgs {
            format: ExportFormat::Dot,
            dot_full: false,
            dot_sample_interval: 100,
        };
        let headers = vec![
            dot_header("genesis", "genesis", Some(0), 1, false),
            dot_header("linear", "genesis", Some(1), 1, false),
            dot_header("sample", "linear", Some(100), 1, false),
            dot_header("fork", "sample", Some(101), 2, false),
            dot_header("tip", "fork", Some(102), 0, true),
        ];

        let included = included_dot_headers(&headers, &args);

        assert!(included.contains("genesis"));
        assert!(!included.contains("linear"));
        assert!(included.contains("sample"));
        assert!(included.contains("fork"));
        assert!(included.contains("tip"));
    }

    #[test]
    fn compact_dot_keeps_immediate_children_of_forks() {
        let args = ExportArgs {
            format: ExportFormat::Dot,
            dot_full: false,
            dot_sample_interval: 100,
        };
        let headers = vec![
            dot_header("fork", "parent", Some(185571), 2, false),
            dot_header("main-child", "fork", Some(185572), 1, false),
            dot_header("fork-child", "fork", Some(185572), 1, false),
            dot_header("later-main-tip", "main-child", Some(185573), 0, true),
            dot_header("later-fork-tip", "fork-child", Some(185573), 0, true),
        ];

        let included = included_dot_headers(&headers, &args);

        assert!(included.contains("fork"));
        assert!(included.contains("main-child"));
        assert!(included.contains("fork-child"));
    }

    #[test]
    fn full_dot_keeps_every_header() {
        let args = ExportArgs {
            format: ExportFormat::Dot,
            dot_full: true,
            dot_sample_interval: 100,
        };
        let headers = vec![
            dot_header("genesis", "genesis", Some(0), 1, false),
            dot_header("linear", "genesis", Some(1), 1, false),
        ];

        let included = included_dot_headers(&headers, &args);

        assert!(included.contains("genesis"));
        assert!(included.contains("linear"));
    }

    #[test]
    fn html_export_embeds_compact_browser_graph() {
        let db = Connection::open_in_memory().unwrap();
        init_db(&db).unwrap();
        db.execute(
            "INSERT INTO headers(hash, parent_hash, height, raw_header_hex, first_seen, last_seen)
             VALUES ('genesis', 'genesis', 0, '', 1, 1),
                    ('linear', 'genesis', 1, '', 1, 1),
                    ('sample', 'linear', 100, '', 1, 1),
                    ('tip', 'sample', 101, '', 1, 1)",
            [],
        )
        .unwrap();
        db.execute(
            "INSERT INTO peers(id, addr, first_seen, last_seen) VALUES (1, '127.0.0.1:8233', 1, 1)",
            [],
        )
        .unwrap();
        db.execute(
            "INSERT INTO peer_tips(peer_id, tip_hash, tip_height, observed_at) VALUES (1, 'tip', 101, 1)",
            [],
        )
        .unwrap();

        let args = ExportArgs {
            format: ExportFormat::Html,
            dot_full: false,
            dot_sample_interval: 100,
        };

        let html = export_html(&db, &args).unwrap();

        assert!(html.starts_with("<!doctype html>"));
        assert!(html.contains("Crosslink Fork Tracker"));
        assert!(html.contains("\"nodes\":"));
        assert!(html.contains("\"edges\":"));
        assert!(html.contains("\"target\":\"tip\""));
    }

    fn dot_header(
        hash: &str,
        parent_hash: &str,
        height: Option<u32>,
        child_count: i64,
        is_tip: bool,
    ) -> DotHeader {
        DotHeader {
            hash: hash.to_string(),
            parent_hash: parent_hash.to_string(),
            height,
            current_peer_count: 0,
            observed_peer_count: 0,
            child_count,
            is_tip,
        }
    }

    #[test]
    fn initializes_database_schema() {
        let db = Connection::open_in_memory().unwrap();
        init_db(&db).unwrap();

        let table_count: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'peers'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(table_count, 1);
    }

    #[test]
    fn local_tip_marker_matches_only_exact_tip() {
        let db = Connection::open_in_memory().unwrap();
        init_db(&db).unwrap();
        let peer_id = upsert_peer_label(&db, "local-rpc:127.0.0.1:8232").unwrap();
        let tip_hash =
            parse_hash("00000001f53a5e284393dfecf2a2405f62c07e2503047a28e2d1b6e76b25f863").unwrap();
        let other_hash =
            parse_hash("00000001dbbb8b26eb92003086c5bd854e16d9f16e2e5b4fcc007b6b0ae57be3").unwrap();

        assert!(!stored_peer_tip_matches(&db, peer_id, Height(3299), tip_hash).unwrap());

        mark_peer_tip_observed(&db, peer_id, Height(3299), tip_hash).unwrap();

        assert!(stored_peer_tip_matches(&db, peer_id, Height(3299), tip_hash).unwrap());
        assert!(!stored_peer_tip_matches(&db, peer_id, Height(3300), tip_hash).unwrap());
        assert!(!stored_peer_tip_matches(&db, peer_id, Height(3299), other_hash).unwrap());
    }

    #[test]
    fn stored_peers_are_reused_as_seeds() {
        let db = Connection::open_in_memory().unwrap();
        init_db(&db).unwrap();
        let peer = "203.0.113.10:8233".parse().unwrap();

        upsert_peer_seen(&db, peer).unwrap();

        assert_eq!(stored_peer_addrs(&db).unwrap(), vec![peer]);
    }

    #[test]
    fn local_rpc_peer_label_is_exported_but_not_reused_as_remote_seed() {
        let db = Connection::open_in_memory().unwrap();
        init_db(&db).unwrap();

        let peer_id = upsert_peer_label(&db, "local-rpc:127.0.0.1:8232").unwrap();
        update_peer_success(&db, peer_id, "local-rpc", "local-rpc", Height(42)).unwrap();

        let peers = export_json(&db).unwrap();

        assert!(stored_peer_addrs(&db).unwrap().is_empty());
        assert_eq!(peers["peers"][0]["addr"], "local-rpc:127.0.0.1:8232");
        assert_eq!(peers["peers"][0]["advertised_height"], 42);
    }

    #[test]
    fn remote_discovered_private_addrs_are_not_safe_by_default() {
        assert!(!is_safe_remote_discovered_addr(
            "127.0.0.1:8233".parse().unwrap()
        ));
        assert!(!is_safe_remote_discovered_addr(
            "192.168.1.1:8233".parse().unwrap()
        ));
        assert!(is_safe_remote_discovered_addr(
            "8.8.8.8:8233".parse().unwrap()
        ));
    }

    fn default_crawl_args() -> CrawlArgs {
        CrawlArgs {
            rpc_addr: "127.0.0.1:8232".parse().unwrap(),
            peer: Vec::new(),
            network: NetworkArg::CrosslinkTestnet0,
            user_agent: String::new(),
            connect_timeout_secs: 10,
            max_peers: 500,
            concurrency: 4,
            max_headers_per_peer: 10000,
            rounds: 3,
            allow_private_discovered_peers: false,
        }
    }
}
