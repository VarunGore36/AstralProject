use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;

use astra_exec::{LimitOrder, MODEL_VERSION, Side, probe_capture};
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

    let report =
        probe_capture(&cli.input, 7, &order, cli.fee_bps).map_err(|error| error.to_string())?;

    println!("input       {}", cli.input.display());
    println!(
        "order       {} {} x {}",
        match cli.side {
            SideArg::Buy => "buy",
            SideArg::Sell => "sell",
        },
        order.price,
        order.quantity
    );
    println!("fee_bps     {}", cli.fee_bps);
    println!("trades      {}", report.trades);
    println!("gaps        {}", report.gaps);
    match report.outcome {
        astra_exec::OrderOutcome::Filled {
            fill_price,
            fee,
            slippage,
            print_seq,
        } => {
            println!("result      filled");
            println!("fill_price  {fill_price}");
            println!("fee         {fee}");
            println!("slippage    {slippage}");
            println!("print_seq   {print_seq}");
        }
        astra_exec::OrderOutcome::Unfilled { reason } => {
            println!(
                "result      unfilled ({})",
                match reason {
                    astra_exec::UnfilledReason::NoThroughPrint => "no-through-print",
                    astra_exec::UnfilledReason::VoidedByGap => "voided-by-gap",
                }
            );
        }
    }
    println!("model       {MODEL_VERSION}");

    Ok(())
}
