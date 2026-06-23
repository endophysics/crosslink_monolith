use zebra_network::PeerSocketAddr;

/// A currently connected peer from Zebra's live peer set.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CrosslinkConnectedPeer {
    /// Transient peer-set address identifying this connection while it remains open.
    pub addr: PeerSocketAddr,

    /// Inbound (true) or outbound (false), when known.
    pub inbound: bool,

    /// Whether this peer is currently ready to receive diagnostic requests.
    pub ready: bool,

    /// Peer user agent from the version handshake.
    pub user_agent: String,

    /// Negotiated network protocol version.
    pub negotiated_version: u32,

    /// Peer-advertised chain height from the version handshake.
    pub advertised_height: u32,

    /// Hex-encoded service flags advertised by the peer.
    pub services: String,
}

/// Response type for `crosslink_getconnectedpeers`.
pub type CrosslinkConnectedPeersResponse = Vec<CrosslinkConnectedPeer>;

/// Response type for `crosslink_getpeerheaders`.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CrosslinkPeerHeadersResponse {
    /// Transient peer-set address that answered the request.
    pub peer: PeerSocketAddr,

    /// Hex-encoded serialized block headers returned by the peer.
    pub headers: Vec<String>,
}
