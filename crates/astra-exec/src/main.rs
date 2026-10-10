use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;

use astra_exec::{LimitOrder, Side, probe_capture};
use astra_types::Fixed;
use clap::Parser;

#[derive(Parser)]
#[command(name = "astra-exec", version)]
#[command(about = "Probe a resting limit order over a capture's trade prints")]
struct Cli {
    #[arg(long, value_name = "DIR")]
    input: PathBuf,
    #[arg(long, value_name = "SIDE")]
    side: SideArg,
    #[arg(long, value_name = "PRICE")]
    price: FixedArg,
    #[arg(long, value_name = "QTY")]
    quantity: FixedArg,
    /// Maker fee in basis points. Required: no silent default.
    #[arg(long, value_name = "BPS")]
    fee_bps: u32,
    /// Replay seed. The collector ignores randomness, so any seed replays
    /// identically — the flag exists to say so explicitly, not by default.
    #[arg(long, value_name = "SEED", default_value_t = 7)]
    seed: u64,
}

#[derive(Clone, Copy)]
enum SideArg {
    Buy,
    Sell,
}

impl FromStr for SideArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "buy" => Ok(SideArg::Buy),
            "sell" => Ok(SideArg::Sell),
            other => Err(format!("side must be buy or sell, saw {other}")),
        }
    }
}

#[derive(Clone, Copy)]
struct FixedArg(Fixed);

impl FromStr for FixedArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse()
            .map(FixedArg)
            .map_err(|error| format!("bad decimal {s:?}: {error}"))
    }
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("astra-exec: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<(), String> {
    let order = LimitOrder {
        side: match cli.side {
            SideArg::Buy => Side::Buy,
            SideArg::Sell => Side::Sell,
        },
        price: cli.price.0,
        quantity: cli.quantity.0,
    };

    let report = probe_capture(&cli.input, cli.seed, &order, cli.fee_bps)
        .map_err(|error| error.to_string())?;
    let side = match cli.side {
        SideArg::Buy => "buy",
        SideArg::Sell => "sell",
    };

    print!(
        "{}",
        astra_exec::format_probe_report(&cli.input, side, &order, cli.fee_bps, &report)
    );

    Ok(())
}
