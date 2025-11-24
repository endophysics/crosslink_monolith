use std::sync::{Arc, Mutex};

fn main() {
    let wallet_state: Arc<Mutex<wallet::WalletState>> = Arc::<Mutex::<wallet::WalletState>>::new(Mutex::<wallet::WalletState>::new(wallet::WalletState { balance: 0 }));
    visualizer_zcash::main_thread_run_program(wallet_state);
}
