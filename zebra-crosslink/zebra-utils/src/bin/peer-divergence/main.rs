//! Find the first block where a local Zebra node and a remote peer diverge.
//!
//! The local chain is queried with Zebra's JSON-RPC `getblockhash(height)` method.
//! The remote peer is queried with the standard Zcash P2P `getheaders` flow via
//! Zebra's isolated peer connection API.

use std::{net::SocketAddr, str::FromStr, time::Duration};

use color_eyre::{
    eyre::{ensure, eyre, Result, WrapErr},
    Help,
};
use serde_json::Value;
use structopt::StructOpt;
use tokio::time::timeout;
use tower::{Service, ServiceExt};

use zebra_chain::{
    block::{self, Height},
    parameters::{testnet, Magic, Network},
};
use zebra_network::{connect_isolated_tcp_direct, Request, Response};
use zebra_node_services::rpc_client::RpcRequestClient;
use zebra_utils::init_tracing;

/// Command-line options for `peer-divergence`.
#[derive(Clone, Debug, StructOpt)]
struct Args {
    /// Network used for the remote peer handshake.
    ///
    /// Supported values: crosslink-testnet-0, crosslink-testnet,
    /// crosslink-regtestnet, testnet, mainnet.
    #[structopt(long, default_value = "crosslink-testnet-0")]
    network: NetworkArg,

    /// Remote peer address in IP:port form.
    ///
    /// In the default mode, this is the peer used for divergence checking. In
    /// `--select-longest` mode, it is included as one of the candidates.
    #[structopt(long)]
    peer: Option<SocketAddr>,

    /// Candidate remote peer address in IP:port form.
    ///
    /// Use with `--select-longest`. Can be supplied multiple times.
    #[structopt(long)]
    candidate_peer: Vec<SocketAddr>,

    /// Connect to all candidates and select the highest advertised chain height.
    ///
    /// The advertised height comes from the remote peer's version handshake. The
    /// selected peer is then verified by the normal header comparison flow.
    #[structopt(long)]
    select_longest: bool,

    /// Local Zebra JSON-RPC address.
    #[structopt(long, default_value = "127.0.0.1:8232")]
    rpc_addr: SocketAddr,

    /// User agent sent on the isolated remote peer connection.
    #[structopt(long, default_value = "")]
    user_agent: String,

    /// Timeout for each remote peer connection attempt, in seconds.
    #[structopt(long, default_value = "10")]
    connect_timeout_secs: u64,

    /// Start from a trusted common local height instead of genesis.
    ///
    /// This speeds up near-tip checks, but it must be a height whose local hash
    /// the remote peer also has on its best chain.
    #[structopt(long, default_value = "0")]
    trusted_common_height: Height,

    /// Stop comparing after this local height. Defaults to the local tip.
    #[structopt(long)]
    max_height: Option<Height>,
}

#[derive(Copy, Clone, Debug)]
struct CommonBlock {
    height: Height,
    hash: block::Hash,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct CandidateSummary {
    addr: SocketAddr,
    advertised_height: Height,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum NetworkArg {
    /// Default Crosslink Testnet 0, using magic bytes `ClT0`.
    CrosslinkTestnet0,
    /// Crosslink Testnet, using magic bytes `ClTn`.
    CrosslinkTestnet,
    /// Local Crosslink regtest-style network, using magic bytes `ClRn`.
    CrosslinkRegtestnet,
    /// Zebra's default public Zcash testnet.
    Testnet,
    /// Zebra's mainnet.
    Mainnet,
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
            "testnet" => Ok(Self::Testnet),
            "mainnet" => Ok(Self::Mainnet),
            _ => Err(format!(
                "invalid network {value:?}; expected crosslink-testnet-0, crosslink-testnet, crosslink-regtestnet, testnet, or mainnet"
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
            Self::Testnet => Network::new_default_testnet(),
            Self::Mainnet => Network::Mainnet,
        }
    }
}

fn crosslink_testnet(network_magic: Magic) -> Network {
    testnet::Parameters::build()
        .with_network_magic(network_magic)
        .with_slow_start_interval(Height(0))
        .to_network()
}

#[tokio::main]
#[allow(clippy::print_stdout, clippy::print_stderr)]
async fn main() -> Result<()> {
    init_tracing();
    color_eyre::install()?;

    let args = Args::from_args();
    run(args).await
}

async fn run(args: Args) -> Result<()> {
    args.validate()?;
    let network = args.network.to_network();

    let rpc_client = RpcRequestClient::new(args.rpc_addr);
    let local_tip = local_tip_height(&rpc_client)
        .await
        .wrap_err("failed to query local tip height")
        .with_suggestion(|| "Is zebrad running with JSON-RPC enabled at --rpc-addr?")?;
    let max_height = args.max_height.unwrap_or(local_tip);

    ensure!(
        args.trusted_common_height <= max_height,
        "trusted common height {:?} is above max height {:?}",
        args.trusted_common_height,
        max_height,
    );
    ensure!(
        max_height <= local_tip,
        "max height {:?} is above the local tip {:?}",
        max_height,
        local_tip,
    );

    let trusted_common_hash = local_block_hash(&rpc_client, args.trusted_common_height).await?;
    let mut common = CommonBlock {
        height: args.trusted_common_height,
        hash: trusted_common_hash,
    };

    let (peer_addr, mut peer) = connect_peer_for_check(&args, &network).await?;
    eprintln!(
        "checking remote peer {} on {} from trusted common block {} {}",
        peer_addr, network, common.height.0, common.hash,
    );

    loop {
        if common.height >= max_height {
            println!(
                "no divergence found through height {} ({})",
                common.height.0, common.hash,
            );
            return Ok(());
        }

        let headers = request_headers(&mut peer, common.hash).await?;
        if headers.is_empty() {
            println!("{}", empty_headers_message(common, max_height));
            return Ok(());
        }

        for counted_header in headers {
            let remote_height = next_height(common.height)?;
            let remote_hash = counted_header.header.hash();

            ensure!(
                counted_header.header.previous_block_hash == common.hash,
                "remote peer did not extend expected common block at height {}: expected previous hash {}, got {}",
                remote_height.0,
                common.hash,
                counted_header.header.previous_block_hash,
            );

            if remote_height > max_height {
                println!(
                    "remote peer extends past requested max height {}; no divergence found through {} ({})",
                    max_height.0, common.height.0, common.hash,
                );
                return Ok(());
            }

            let local_hash = local_block_hash(&rpc_client, remote_height).await?;
            if remote_hash != local_hash {
                println!("divergence found at height {}", remote_height.0);
                println!(
                    "previous common: height {} hash {}",
                    common.height.0, common.hash
                );
                println!("local hash:  {local_hash}");
                println!("remote hash: {remote_hash}");
                return Ok(());
            }

            common = CommonBlock {
                height: remote_height,
                hash: remote_hash,
            };
        }
    }
}

impl Args {
    fn validate(&self) -> Result<()> {
        if self.select_longest {
            ensure!(
                self.peer.is_some() || !self.candidate_peer.is_empty(),
                "--select-longest requires --peer or at least one --candidate-peer",
            );
        } else {
            ensure!(
                self.peer.is_some(),
                "--peer is required unless --select-longest is used",
            );
            ensure!(
                self.candidate_peer.is_empty(),
                "--candidate-peer requires --select-longest",
            );
        }

        ensure!(
            connect_timeout(self.connect_timeout_secs).is_some(),
            "--connect-timeout-secs must be greater than 0",
        );

        Ok(())
    }
}

async fn request_headers(
    peer: &mut zebra_network::Client,
    known_hash: block::Hash,
) -> Result<Vec<block::CountedHeader>> {
    match peer
        .ready()
        .await
        .map_err(|error| eyre!(error.to_string()))
        .wrap_err("remote peer was not ready for a header request")?
        .call(Request::FindHeaders {
            known_blocks: vec![known_hash],
            stop: None,
        })
        .await
        .map_err(|error| eyre!(error.to_string()))
        .wrap_err("remote peer header request failed")?
    {
        Response::Nil => Ok(Vec::new()),
        Response::BlockHeaders(headers) => Ok(headers),
        response => Err(eyre!(
            "unexpected response to FindHeaders: {}",
            response.command()
        )),
    }
}

async fn connect_peer_for_check(
    args: &Args,
    network: &Network,
) -> Result<(SocketAddr, zebra_network::Client)> {
    if args.select_longest {
        select_longest_peer(args, network).await
    } else {
        ensure!(
            args.candidate_peer.is_empty(),
            "--candidate-peer requires --select-longest",
        );

        let peer_addr = args
            .peer
            .ok_or_else(|| eyre!("--peer is required unless --select-longest is used"))?;
        let peer = connect_peer(
            network,
            peer_addr,
            args.user_agent.clone(),
            args.connect_timeout_secs,
        )
        .await?;

        Ok((peer_addr, peer))
    }
}

async fn select_longest_peer(
    args: &Args,
    network: &Network,
) -> Result<(SocketAddr, zebra_network::Client)> {
    let candidate_addrs = candidate_addrs(args);
    ensure!(
        !candidate_addrs.is_empty(),
        "--select-longest requires --peer or at least one --candidate-peer",
    );

    let mut best_peer: Option<(CandidateSummary, zebra_network::Client)> = None;
    let mut connection_failures = Vec::new();
    for candidate_addr in candidate_addrs {
        eprintln!("probing candidate peer {candidate_addr} on {network}");

        match connect_peer(
            network,
            candidate_addr,
            args.user_agent.clone(),
            args.connect_timeout_secs,
        )
        .await
        {
            Ok(peer) => {
                let summary = CandidateSummary {
                    addr: candidate_addr,
                    advertised_height: peer.connection_info.remote.start_height,
                };
                eprintln!(
                    "candidate peer {} advertised height {}",
                    summary.addr, summary.advertised_height.0,
                );

                if is_better_candidate(best_peer.as_ref().map(|(summary, _peer)| *summary), summary)
                {
                    best_peer = Some((summary, peer));
                }
            }
            Err(error) => {
                let failure = format_peer_connection_failure(candidate_addr, &error);
                eprintln!("candidate peer failed: {failure}");
                connection_failures.push(failure);
            }
        }
    }

    let (best_summary, best_peer) = best_peer.ok_or_else(|| {
        eyre!(all_candidate_connection_failures_message(
            &connection_failures
        ))
        .with_suggestion(|| {
            "If these peers came from testnet_1.toml/testnet_2.toml, retry with \
             --network crosslink-regtestnet or ZEBRA_NETWORK=crosslink-regtestnet; \
             sam_mac_multiplayer_config_*.toml peers use crosslink-testnet."
        })
    })?;
    eprintln!(
        "selected peer {} with advertised height {}",
        best_summary.addr, best_summary.advertised_height.0,
    );

    Ok((best_summary.addr, best_peer))
}

fn format_peer_connection_failure(peer_addr: SocketAddr, error: &color_eyre::Report) -> String {
    format!("{peer_addr}: {error:?}")
}

fn all_candidate_connection_failures_message(connection_failures: &[String]) -> String {
    if connection_failures.is_empty() {
        return "all candidate peer connections failed".to_string();
    }

    format!(
        "all candidate peer connections failed:\n  - {}",
        connection_failures.join("\n  - "),
    )
}

async fn connect_peer(
    network: &Network,
    peer_addr: SocketAddr,
    user_agent: String,
    connect_timeout_secs: u64,
) -> Result<zebra_network::Client> {
    let connect_timeout = connect_timeout(connect_timeout_secs)
        .ok_or_else(|| eyre!("--connect-timeout-secs must be greater than 0"))?;

    timeout(
        connect_timeout,
        connect_isolated_tcp_direct(network, peer_addr, user_agent),
    )
    .await
    .map_err(|_elapsed| eyre!("timed out connecting to remote peer {peer_addr}"))?
    .map_err(|error| eyre!(error.to_string()))
    .wrap_err_with(|| format!("failed to connect to remote peer {peer_addr}"))
}

fn connect_timeout(connect_timeout_secs: u64) -> Option<Duration> {
    (connect_timeout_secs > 0).then(|| Duration::from_secs(connect_timeout_secs))
}

fn candidate_addrs(args: &Args) -> Vec<SocketAddr> {
    let mut candidate_addrs = Vec::new();

    if let Some(peer_addr) = args.peer {
        candidate_addrs.push(peer_addr);
    }

    for candidate_addr in &args.candidate_peer {
        if !candidate_addrs.contains(candidate_addr) {
            candidate_addrs.push(*candidate_addr);
        }
    }

    candidate_addrs
}

fn is_better_candidate(current: Option<CandidateSummary>, candidate: CandidateSummary) -> bool {
    current
        .map(|current| candidate.advertised_height > current.advertised_height)
        .unwrap_or(true)
}

async fn local_tip_height(client: &RpcRequestClient) -> Result<Height> {
    let info: Value = client
        .json_result_from_call("getblockchaininfo", "[]")
        .await
        .map_err(|error| eyre!(error))?;

    let blocks = info
        .get("blocks")
        .and_then(Value::as_u64)
        .ok_or_else(|| eyre!("getblockchaininfo response has no numeric blocks field"))?;

    height_from_u64(blocks)
}

async fn local_block_hash(client: &RpcRequestClient, height: Height) -> Result<block::Hash> {
    let hash: String = client
        .json_result_from_call("getblockhash", format!("[{}]", height.0))
        .await
        .map_err(|error| eyre!(error))?;

    block::Hash::from_str(&hash)
        .wrap_err_with(|| format!("invalid block hash at height {}", height.0))
}

fn height_from_u64(height: u64) -> Result<Height> {
    ensure!(
        height <= Height::MAX.0.into(),
        "height {height} exceeds Zebra's maximum block height",
    );

    Ok(Height(height as u32))
}

fn next_height(height: Height) -> Result<Height> {
    let next_height = height
        .0
        .checked_add(1)
        .ok_or_else(|| eyre!("height {} cannot be incremented", height.0))?;

    ensure!(
        next_height <= Height::MAX.0,
        "height {next_height} exceeds Zebra's maximum block height",
    );

    Ok(Height(next_height))
}

fn empty_headers_message(common: CommonBlock, max_height: Height) -> String {
    if common.height >= max_height {
        format!(
            "no divergence found through height {} ({})",
            common.height.0, common.hash,
        )
    } else {
        format!(
            "remote peer returned no headers after height {} ({}); no divergence found through that height, but requested max height {} was not reached",
            common.height.0, common.hash, max_height.0,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_maximum_zebra_height() {
        assert_eq!(height_from_u64(Height::MAX.0.into()).unwrap(), Height::MAX);
    }

    #[test]
    fn rejects_height_above_zebra_maximum() {
        assert!(height_from_u64(u64::from(Height::MAX.0) + 1).is_err());
    }

    #[test]
    fn increments_height_until_maximum() {
        assert_eq!(next_height(Height(0)).unwrap(), Height(1));
        assert_eq!(next_height(Height(Height::MAX.0 - 1)).unwrap(), Height::MAX);
    }

    #[test]
    fn rejects_increment_above_maximum() {
        assert!(next_height(Height::MAX).is_err());
    }

    #[test]
    fn empty_headers_report_remote_behind() {
        let common = CommonBlock {
            height: Height(10),
            hash: block::Hash([1; 32]),
        };

        let message = empty_headers_message(common, Height(20));

        assert!(message.contains("remote peer returned no headers after height 10"));
        assert!(message.contains("requested max height 20 was not reached"));
    }

    #[test]
    fn candidate_addrs_include_peer_and_deduplicate_candidates() {
        let peer_addr = "127.0.0.1:8233".parse().unwrap();
        let other_addr = "127.0.0.2:8233".parse().unwrap();
        let args = Args {
            network: NetworkArg::Mainnet,
            peer: Some(peer_addr),
            candidate_peer: vec![peer_addr, other_addr, other_addr],
            select_longest: true,
            rpc_addr: "127.0.0.1:8232".parse().unwrap(),
            user_agent: String::new(),
            connect_timeout_secs: 10,
            trusted_common_height: Height(0),
            max_height: None,
        };

        assert_eq!(candidate_addrs(&args), vec![peer_addr, other_addr]);
    }

    #[test]
    fn higher_advertised_height_is_better_candidate() {
        let current = CandidateSummary {
            addr: "127.0.0.1:8233".parse().unwrap(),
            advertised_height: Height(10),
        };
        let shorter = CandidateSummary {
            addr: "127.0.0.2:8233".parse().unwrap(),
            advertised_height: Height(9),
        };
        let longer = CandidateSummary {
            addr: "127.0.0.3:8233".parse().unwrap(),
            advertised_height: Height(11),
        };

        assert!(is_better_candidate(None, current));
        assert!(!is_better_candidate(Some(current), shorter));
        assert!(is_better_candidate(Some(current), longer));
    }

    #[test]
    fn validates_default_mode_requires_peer_before_rpc() {
        let args = Args {
            network: NetworkArg::Mainnet,
            peer: None,
            candidate_peer: Vec::new(),
            select_longest: false,
            rpc_addr: "127.0.0.1:8232".parse().unwrap(),
            user_agent: String::new(),
            connect_timeout_secs: 10,
            trusted_common_height: Height(0),
            max_height: None,
        };

        let error = args.validate().unwrap_err().to_string();

        assert!(error.contains("--peer is required"));
    }

    #[test]
    fn validates_candidate_peers_require_select_longest() {
        let args = Args {
            network: NetworkArg::Mainnet,
            peer: Some("127.0.0.1:8233".parse().unwrap()),
            candidate_peer: vec!["127.0.0.2:8233".parse().unwrap()],
            select_longest: false,
            rpc_addr: "127.0.0.1:8232".parse().unwrap(),
            user_agent: String::new(),
            connect_timeout_secs: 10,
            trusted_common_height: Height(0),
            max_height: None,
        };

        let error = args.validate().unwrap_err().to_string();

        assert!(error.contains("--candidate-peer requires --select-longest"));
    }

    #[test]
    fn validates_select_longest_requires_candidates() {
        let args = Args {
            network: NetworkArg::Mainnet,
            peer: None,
            candidate_peer: Vec::new(),
            select_longest: true,
            rpc_addr: "127.0.0.1:8232".parse().unwrap(),
            user_agent: String::new(),
            connect_timeout_secs: 10,
            trusted_common_height: Height(0),
            max_height: None,
        };

        let error = args.validate().unwrap_err().to_string();

        assert!(error.contains("--select-longest requires"));
    }

    #[test]
    fn validates_positive_connect_timeout() {
        assert_eq!(connect_timeout(10), Some(Duration::from_secs(10)));
        assert_eq!(connect_timeout(0), None);
    }

    #[test]
    fn all_candidate_connection_failures_include_peer_details() {
        let message = all_candidate_connection_failures_message(&[
            "127.0.0.1:8233: handshake failed".to_string(),
            "127.0.0.1:8235: connection refused".to_string(),
        ]);

        assert!(message.contains("all candidate peer connections failed"));
        assert!(message.contains("127.0.0.1:8233: handshake failed"));
        assert!(message.contains("127.0.0.1:8235: connection refused"));
    }

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
}
