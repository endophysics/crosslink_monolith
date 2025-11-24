//! Internal wallet

use orchard::note_encryption::{CompactAction};
use tokio_rustls::rustls;
use tonic::client::GrpcService;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use zcash_note_encryption::try_compact_note_decryption;
use std::future::Future;
use std::sync::{Arc, Mutex};

use rustls::CertificateError;
use rustls::client::danger::ServerCertVerified;
use rustls::client::danger::ServerCertVerifier;
use rustls::crypto::WebPkiSupportedAlgorithms;
use rustls::crypto::ring;
use rustls::crypto::CryptoProvider;
use rustls::crypto::verify_tls12_signature;
use rustls::crypto::verify_tls13_signature;
use tokio::runtime::Builder;
use zcash_client_backend::{
    address::{
        UnifiedAddress,
    },
    encoding::AddressCodec,
    keys::{
        UnifiedAddressRequest,
        UnifiedIncomingViewingKey,
        UnifiedSpendingKey,
    },
    proto::{
        compact_formats::CompactTx,
        service::{
            BlockId,
            BlockRange,
            ChainSpec,
            Duration,
            GetAddressUtxosArg,
            Empty,
            LightdInfo,
            compact_tx_streamer_client::CompactTxStreamerClient
        }
    },
};
use zcash_primitives::{
    consensus::{
        MAIN_NETWORK,
        TEST_NETWORK,
        NetworkType,
        Parameters,
    },
};
use zcash_transparent::{
    address::TransparentAddress,
    keys::{
        IncomingViewingKey,
        TransparentKeyScope,
    },
};

fn the_future_is_now<F: Future>(future: F) -> F::Output {
    Builder::new_current_thread()
        .enable_time()
        .enable_io()
        .build()
        .unwrap()
        .block_on(future)
}

async fn wait_for_zainod() {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(500));
    for _ in 0..10 {
        let client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(2))
            .timeout(std::time::Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();

        let request_body = r#"{"jsonrpc":"2.0","method":"getinfo","params":[],"id":1}"#;
        let request_builder = client
            .post("http://localhost:18232")
            .header("Content-Type", "application/json")
            .body(request_body);

        if let Ok(res) = request_builder.send().await {
            if res.status().is_success() {
                println!("ZAINO IS READY: {}", res.text().await.unwrap());
                return;
            }
        }

        interval.tick().await;
    }
}

pub struct WalletState {
    pub balance: i64, // in zats
}


impl WalletState {
    pub fn new() -> Self {
        WalletState {
            balance: 0,
        }
    }
}

pub fn wallet_main(wallet_state: Arc<Mutex<WalletState>>) {
    the_future_is_now(async {
        println!("waiting for zaino to be ready...");
        wait_for_zainod().await;
    });

    let cdb = zcash_client_sqlite::BlockDb::for_path(":memory:").unwrap();
    zcash_client_sqlite::chain::init::init_cache_database(&cdb).unwrap();

    let network = &TEST_NETWORK;

    // miner/faucet wallet setup
    let miner_t_addr = {
        use secrecy::ExposeSecret;

        let phrase = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
        let mnemonic = bip39::Mnemonic::parse(phrase).unwrap();
        let bip39_passphrase = ""; // optional
        let seed64 = mnemonic.to_seed(bip39_passphrase);
        let seed = secrecy::SecretVec::new(seed64[..32].to_vec());

        // 2. Derive Unified Spending Key (USK) from seed
        let account_id = zcash_primitives::zip32::AccountId::try_from(0).unwrap();
        let usk = UnifiedSpendingKey::from_seed(network, seed.expose_secret(), account_id).unwrap();
        let (t_addr, child_index) = usk.transparent()
            .to_account_pubkey()
            .derive_external_ivk()
            .unwrap()
            .default_address();
        let account_pubkey = usk.transparent().to_account_pubkey();
        let address_pubkey = account_pubkey.derive_address_pubkey(TransparentKeyScope::EXTERNAL, child_index).unwrap();
        TransparentAddress::from_pubkey(&address_pubkey)
    };
    let miner_t_addr_str = miner_t_addr.encode(network);
    println!("Faucet miner t-address: {}", miner_t_addr_str);

    let mut seen_block_height = 0;
    loop {
        let mut sum = 0;
        let mut count = 0;
        the_future_is_now(async {
            let mut client = CompactTxStreamerClient::new(Channel::from_static("http://localhost:18233").connect().await.unwrap());
            // let block_range = BlockRange{
            //     start: Some(BlockId{ height: seen_block_height, hash: Vec::new() }),
            //     end: Some(BlockId{ height: seen_block_height, hash: Vec::new() }),
            // };
            let range = match client.get_address_utxos(GetAddressUtxosArg {
                addresses: vec![miner_t_addr_str.to_owned()],
                start_height: seen_block_height,
                max_entries: 0
            }).await {
                Err(err) => println!("******* GET UTXOS ERROR: {:?}", err),
                Ok(res) => {
                    let utxos = res.into_inner().address_utxos;
                    count = utxos.len();
                    for utxo in utxos {
                        sum += utxo.value_zat;
                    }
                }
            };

            let zec_full = sum / 100_000_000;
            let zec_part = sum % 100_000_000;
            println!("miner {} has {} UTXOs with {} zats = {}.{} cTAZ", miner_t_addr_str, count, sum, zec_full, zec_part);

            wallet_state.lock().unwrap().balance = sum;
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        });
    }

    /*
    let uivk = UnifiedIncomingViewingKey::decode(&MAIN_NETWORK, "uivk1u7ty6ntudngulxlxedkad44w7g6nydknyrdsaw0jkacy0z8k8qk37t4v39jpz2qe3y98q4vs0s05f4u2vfj5e9t6tk9w5r0a3p4smfendjhhm5au324yvd84vsqe664snjfzv9st8z4s8faza5ytzvte5s9zruwy8vf0ze0mhq7ldfl2js8u58k5l9rjlz89w987a9akhgvug3zaz55d5h0d6ndyt4udl2ncwnm30pl456frnkj").unwrap();

    let ua = uivk.default_address(UnifiedAddressRequest::SHIELDED).unwrap().0;
    println!("UA: {}", ua.encode(&MAIN_NETWORK));

    let https_uri = "https://na.zec.rocks:443";
    let cert = include_bytes!("../na.zec.rocks-leaf.der");

    let transactions = the_future_is_now(async move {
        CryptoProvider::install_default(ring::default_provider()).unwrap();

        let mut cfg = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(DerVerifier{
                certificate: cert,
                algorithms: CryptoProvider::get_default()
                    .map(|provider| provider.signature_verification_algorithms)
                    .unwrap(),
            }))
            .with_no_client_auth();

        cfg.alpn_protocols.push(b"h2".to_vec());

        let mut client = CompactTxStreamerClient::new({
            let v = Endpoint::from_shared(https_uri).unwrap();
            let v = v.tls_config(cfg).unwrap();
            v.connect().await.unwrap()
        });
    */

    /*
    let transactions = the_future_is_now(async move {
        let mut client = CompactTxStreamerClient::new({
            let c = Channel::from_shared("127.0.0.1:8080").unwrap();
            c.connect().await.unwrap()
        });

        let block_stream = client.get_block_range(BlockRange{
            start: Some(BlockId{height: 3051998, hash: Vec::new()}),
            end:   Some(BlockId{height: 3052065, hash: Vec::new()}),
        }).await.unwrap();
        let mut block_grpc = block_stream.into_inner();

        let mut blocks = Vec::new();
        loop {
            if let Ok(msg) = block_grpc.message().await {
                if let Some(block) = msg {
                    blocks.push(block);
                    continue;
                }
            }

            break;
        }

        let sapling_ivk = if let Some(ivk) = uivk.sapling() { Some(ivk.prepare()) } else { None };
        let orchard_ivk = if let Some(ivk) = uivk.orchard() { Some(ivk.prepare()) } else { None };

        let mut txs = Vec::new();
        for b in &blocks {
            for tx in &b.vtx {
                let mut transaction_is_ours = false;

                if let Some(ivk) = &sapling_ivk {
                    for sapling_output in &tx.outputs {
                        let Ok(compact_output) = CompactOutputDescription::try_from(sapling_output) else { continue };
                        if let Some((note, _)) = try_sapling_compact_note_decryption(ivk, &compact_output, Zip212Enforcement::On) {
                            println!("Sapling Note: {:#?}", note);
                            transaction_is_ours = true;
                            break;
                        }
                    }
                }

                if let Some(ivk) = &orchard_ivk {
                    for action in &tx.actions {
                        let Ok(compact_action) = CompactAction::try_from(action) else { continue };
                        let domain = OrchardDomain::for_compact_action(&compact_action);
                        if let Some((note, _recipient)) = try_compact_note_decryption(&domain, ivk, &compact_action) {
                            println!("Orchard Note: {:#?}", note);
                            transaction_is_ours = true;
                            break;
                        }
                    }
                }

                if transaction_is_ours {
                    txs.push(tx.clone());
                }
            }
        }

        txs
    });
    */
}

/*
#[derive(Debug)]
struct DerVerifier {
    certificate: &'static [u8],
    algorithms: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for DerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &tonic::transport::CertificateDer<'_>,
        _intermediates: &[tonic::transport::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        if end_entity.as_ref() == self.certificate {
            Ok(ServerCertVerified::assertion())
        }
        else {
            Err(rustls::Error::InvalidCertificate(CertificateError::UnknownIssuer))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &tonic::transport::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &tonic::transport::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}
*/
