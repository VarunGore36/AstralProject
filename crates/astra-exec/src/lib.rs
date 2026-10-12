//! Conservative execution model (`exec-v1`, limit-maker only).
//!
//! Answers one question about a resting limit order replayed over trade
//! prints: did a print trade at or through the limit price, and if so, what
//! maker fee did it cost? Quote movement alone never fills anything; any gap
//! overlapping the order's live window voids it; no print means expiry.
//!
//! This is a pure function over events — no I/O, no clock, no venue calls.
//! Wiring replay output into it (and any calibration against reality) is a
//! later layer. See `docs/execution-model.md` for the full contract.

use std::path::Path;

use astra_types::Fixed;
use serde::Serialize;
use thiserror::Error;

/// Model version stamped on every result. Bump on any rule change.
pub const MODEL_VERSION: &str = "exec-v1";

/// Fee denominator: basis points.
const BPS_DENOMINATOR: i128 = 10_000;

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize)]
pub struct LimitOrder {
    pub side: Side,
    pub price: Fixed,
    pub quantity: Fixed,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Print {
    pub seq: u64,
    pub price: Fixed,
    pub quantity: Fixed,
}

/// Replay output in stream order. Prints come from normalized trade rows;
/// gaps come from synthetic markers. Anything else never reaches this model.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MarketEvent {
    Print(Print),
    Gap,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UnfilledReason {
    /// Stream ended (or input was empty) with no through-print.
    NoThroughPrint,
    /// A gap marker arrived while the order was still live.
    VoidedByGap,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum OrderOutcome {
    Filled {
        /// Always the limit price in v1 (whole order, first through-print).
        fill_price: Fixed,
        /// Maker fee at the required tier. Slippage is 0 by construction and
        /// therefore reported as 0, not omitted.
        fee: Fixed,
        slippage: Fixed,
        /// The print that caused the fill.
        print_seq: u64,
    },
    Unfilled {
        reason: UnfilledReason,
    },
}

#[derive(Clone, PartialEq, Eq, Debug, Error)]
pub enum ExecError {
    #[error("invalid order (price and quantity must be positive): {0}")]
    InvalidOrder(String),
    #[error("fixed-point arithmetic overflowed")]
    ArithmeticOverflow,
}

/// Simulate one resting limit order over a print stream.
///
/// `fee_bps` is required and in basis points on notional (e.g. 5 = 0.05%).
/// There is no default: callers that do not know their tier must pass the
/// taker-with-no-rebate figure explicitly rather than inherit optimism.
pub fn simulate(
    order: &LimitOrder,
    fee_bps: u32,
    events: &[MarketEvent],
) -> Result<OrderOutcome, ExecError> {
    if order.price.raw() <= 0 || order.quantity.raw() <= 0 {
        return Err(ExecError::InvalidOrder(format!(
            "{} x {}",
            order.price, order.quantity
        )));
    }

    for event in events {
        match event {
            MarketEvent::Gap => {
                return Ok(OrderOutcome::Unfilled {
                    reason: UnfilledReason::VoidedByGap,
                });
            }
            MarketEvent::Print(print) => {
                // Corrupt prints (non-positive prices or quantities) can
                // never fill: a fill needs a real counterparty trading a real
                // size at a real price. Skipped, not filled and not fatal —
                // the stream may still hold evidence.
                if print.price.raw() <= 0 || print.quantity.raw() <= 0 {
                    continue;
                }
                let through = match order.side {
                    Side::Buy => print.price.raw() <= order.price.raw(),
                    Side::Sell => print.price.raw() >= order.price.raw(),
                };
                if through {
                    let notional = order
                        .price
                        .checked_mul(order.quantity)
                        .ok_or(ExecError::ArithmeticOverflow)?;
                    return Ok(OrderOutcome::Filled {
                        fill_price: order.price,
                        fee: maker_fee(notional, fee_bps)?,
                        slippage: Fixed::ZERO,
                        print_seq: print.seq,
                    });
                }
            }
        }
    }

    Ok(OrderOutcome::Unfilled {
        reason: UnfilledReason::NoThroughPrint,
    })
}

/// Maker fee on a notional at `fee_bps` basis points, rounded half away
/// from zero. Inputs here are always positive (validated above), so the
/// rounding branch is exact, not a sign convention.
fn maker_fee(notional: Fixed, fee_bps: u32) -> Result<Fixed, ExecError> {
    let scaled = notional
        .raw()
        .checked_mul(fee_bps as i128)
        .ok_or(ExecError::ArithmeticOverflow)?;
    let quotient = scaled / BPS_DENOMINATOR;
    let bump = i128::from((scaled % BPS_DENOMINATOR).abs() >= BPS_DENOMINATOR / 2);
    let raw = quotient
        .checked_add(bump)
        .ok_or(ExecError::ArithmeticOverflow)?;
    Ok(Fixed::from_raw(raw))
}

/// A collected probe run: the trade-print stream plus its gaps, and the
/// order's fate against them.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ProbeReport {
    pub trades: u64,
    pub gaps: u64,
    pub outcome: OrderOutcome,
}

/// Render a probe report exactly as the CLI prints it.
///
/// Same contract as the other report printers: golden-tested, changed only
/// together with its test.
pub fn format_probe_report(
    input: &Path,
    side: &str,
    order: &LimitOrder,
    fee_bps: u32,
    report: &ProbeReport,
) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    let _ = writeln!(out, "input       {}", input.display());
    let _ = writeln!(
        out,
        "order       {side} {} x {}",
        order.price, order.quantity
    );
    let _ = writeln!(out, "fee_bps     {fee_bps}");
    let _ = writeln!(out, "trades      {}", report.trades);
    let _ = writeln!(out, "gaps        {}", report.gaps);
    match &report.outcome {
        OrderOutcome::Filled {
            fill_price,
            fee,
            slippage,
            print_seq,
        } => {
            let _ = writeln!(out, "result      filled");
            let _ = writeln!(out, "fill_price  {fill_price}");
            let _ = writeln!(out, "fee         {fee}");
            let _ = writeln!(out, "slippage    {slippage}");
            let _ = writeln!(out, "print_seq   {print_seq}");
        }
        OrderOutcome::Unfilled { reason } => {
            let _ = writeln!(
                out,
                "result      unfilled ({})",
                match reason {
                    UnfilledReason::NoThroughPrint => "no-through-print",
                    UnfilledReason::VoidedByGap => "voided-by-gap",
                }
            );
        }
    }
    let _ = writeln!(out, "model       {MODEL_VERSION}");

    out
}

#[derive(Debug, Error)]
pub enum ProbeError {
    #[error("replay error: {0}")]
    Replay(#[from] astra_replay::ReplayError),
    #[error("exec error: {0}")]
    Exec(#[from] ExecError),
}

/// Collects the trade-print stream (plus gaps) out of a replay.
///
/// Public so the benchmark harness can compose it with a book strategy in a
/// single replay pass instead of re-reading the capture per probe.
#[derive(Default)]
pub struct TradeCollector {
    pub events: Vec<MarketEvent>,
    pub trades: u64,
    pub gaps: u64,
}

impl astra_replay::Strategy for TradeCollector {
    fn on_book_diff(
        &mut self,
        _event: &astra_replay::BookDiffEvent,
        _ctx: &mut astra_replay::Context,
    ) {
    }
    fn on_snapshot(
        &mut self,
        _event: &astra_replay::SnapshotEvent,
        _ctx: &mut astra_replay::Context,
    ) {
    }
    fn on_trade(&mut self, event: &astra_replay::TradeEvent, _ctx: &mut astra_replay::Context) {
        self.events.push(MarketEvent::Print(Print {
            seq: event.seq,
            price: event.trade.price,
            quantity: event.trade.quantity,
        }));
        self.trades += 1;
    }
    fn on_top_of_book(
        &mut self,
        _event: &astra_replay::TopBookEvent,
        _ctx: &mut astra_replay::Context,
    ) {
    }
    fn on_gap(&mut self, _marker: &astra_types::GapMarker, _ctx: &mut astra_replay::Context) {
        self.events.push(MarketEvent::Gap);
        self.gaps += 1;
    }
}

/// Replay a capture's trade prints through one resting order.
///
/// Book-diff and top-of-book frames pass through the replay untouched by
/// this model (they are somebody else's evidence); gaps void live orders.
pub fn probe_capture(
    input: &Path,
    seed: u64,
    order: &LimitOrder,
    fee_bps: u32,
) -> Result<ProbeReport, ProbeError> {
    let mut collector = TradeCollector::default();
    astra_replay::replay(input, seed, &mut collector)?;
    Ok(ProbeReport {
        trades: collector.trades,
        gaps: collector.gaps,
        outcome: simulate(order, fee_bps, &collector.events)?,
    })
}

/// A trade print with its socket timestamp, for post-fill measurement.
/// `ts_socket_ns` is transport time (when this machine saw the print), the
/// same clock the recorder stamps on every record.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TimedPrint {
    pub seq: u64,
    pub price: Fixed,
    pub quantity: Fixed,
    pub ts_socket_ns: i64,
}

/// Timed replay output in stream order. Gaps void live orders in `simulate`
/// and void pending markout horizons here.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TimedEvent {
    Print(TimedPrint),
    Gap,
}

/// A would-have-filled order with what happened next: per-horizon markouts
/// in basis points, signed so that negative always means adverse selection
/// (the market moved against the fill). `None` means the horizon is
/// unmeasurable — no later print covers it, or a gap crossed the window.
/// Floats are analytics, never money: the fill price and fee stay `Fixed`.
#[derive(Clone, PartialEq, Debug, Serialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum MarkoutOutcome {
    Filled {
        fill_price: Fixed,
        fee: Fixed,
        slippage: Fixed,
        print_seq: u64,
        fill_ts_ns: i64,
        horizons_ns: Vec<u64>,
        markouts_bps: Vec<Option<f64>>,
    },
    Unfilled {
        reason: UnfilledReason,
    },
}

/// Measure adverse selection after a conservative fill.
///
/// The fill itself follows the exact `simulate` rules (through-print only,
/// gaps void, corrupt prints skipped) by delegating to it; only filled
/// orders get markouts. `horizons_ns` are nanosecond offsets from the fill
/// print's socket time; each horizon reads the first later print at or past
/// its mark. A gap anywhere between the fill and a horizon's covering print
/// voids that horizon — prints past a hole describe a market the fill never
/// saw.
pub fn markout(
    order: &LimitOrder,
    fee_bps: u32,
    events: &[TimedEvent],
    horizons_ns: &[u64],
) -> Result<MarkoutOutcome, ExecError> {
    let untimed: Vec<MarketEvent> = events
        .iter()
        .map(|event| match event {
            TimedEvent::Print(print) => MarketEvent::Print(Print {
                seq: print.seq,
                price: print.price,
                quantity: print.quantity,
            }),
            TimedEvent::Gap => MarketEvent::Gap,
        })
        .collect();
    let fill = match simulate(order, fee_bps, &untimed)? {
        OrderOutcome::Filled {
            fill_price,
            fee,
            slippage,
            print_seq,
        } => (fill_price, fee, slippage, print_seq),
        OrderOutcome::Unfilled { reason } => return Ok(MarkoutOutcome::Unfilled { reason }),
    };
    let (fill_price, fee, slippage, print_seq) = fill;

    let fill_index = events
        .iter()
        .position(|event| matches!(event, TimedEvent::Print(print) if print.seq == print_seq))
        .expect("fill print comes from this stream");
    let fill_ts = match &events[fill_index] {
        TimedEvent::Print(print) => print.ts_socket_ns,
        TimedEvent::Gap => unreachable!("fill print comes from this stream"),
    };

    let mut covered: Vec<Option<f64>> = vec![None; horizons_ns.len()];
    let mut pending: Vec<usize> = (0..horizons_ns.len()).collect();
    for event in &events[fill_index + 1..] {
        if pending.is_empty() {
            break;
        }
        match event {
            // Prints past a hole are a new regime: every still-open horizon
            // is unmeasurable, not zero.
            TimedEvent::Gap => break,
            TimedEvent::Print(print) => {
                if print.price.raw() <= 0 || print.quantity.raw() <= 0 {
                    continue;
                }
                pending.retain(|&position| {
                    let mark = fill_ts.saturating_add_unsigned(horizons_ns[position]);
                    if print.ts_socket_ns >= mark {
                        covered[position] = Some(markout_bps(order.side, fill_price, print.price));
                        false
                    } else {
                        true
                    }
                });
            }
        }
    }

    Ok(MarkoutOutcome::Filled {
        fill_price,
        fee,
        slippage,
        print_seq,
        fill_ts_ns: fill_ts,
        horizons_ns: horizons_ns.to_vec(),
        markouts_bps: covered,
    })
}

/// Signed basis points from the fill to a later print: negative is adverse
/// for the filled side in both directions (bought before a fall, or sold
/// before a rise).
fn markout_bps(side: Side, fill_price: Fixed, later_price: Fixed) -> f64 {
    let fill = fill_price.to_f64_for_analytics();
    let later = later_price.to_f64_for_analytics();
    let movement = match side {
        Side::Buy => later - fill,
        Side::Sell => fill - later,
    };
    movement / fill * 10_000.0
}

/// Collects the timestamped trade-print stream (plus gaps) out of a replay.
#[derive(Default)]
pub struct TimedCollector {
    pub events: Vec<TimedEvent>,
    pub trades: u64,
    pub gaps: u64,
}

impl astra_replay::Strategy for TimedCollector {
    fn on_book_diff(
        &mut self,
        _event: &astra_replay::BookDiffEvent,
        _ctx: &mut astra_replay::Context,
    ) {
    }
    fn on_snapshot(
        &mut self,
        _event: &astra_replay::SnapshotEvent,
        _ctx: &mut astra_replay::Context,
    ) {
    }
    fn on_trade(&mut self, event: &astra_replay::TradeEvent, _ctx: &mut astra_replay::Context) {
        self.events.push(TimedEvent::Print(TimedPrint {
            seq: event.seq,
            price: event.trade.price,
            quantity: event.trade.quantity,
            ts_socket_ns: event.ts_socket.unix_nanos(),
        }));
        self.trades += 1;
    }
    fn on_top_of_book(
        &mut self,
        _event: &astra_replay::TopBookEvent,
        _ctx: &mut astra_replay::Context,
    ) {
    }
    fn on_gap(&mut self, _marker: &astra_types::GapMarker, _ctx: &mut astra_replay::Context) {
        self.events.push(TimedEvent::Gap);
        self.gaps += 1;
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct MarkoutReport {
    pub trades: u64,
    pub gaps: u64,
    pub outcome: MarkoutOutcome,
}

/// Replay a capture's trade prints through one resting order and measure
/// what happened next at each horizon. Same evidence rules as the probe;
/// horizons in nanoseconds (parse `1s`/`500ms` style flags at the CLI).
pub fn probe_markout(
    input: &Path,
    seed: u64,
    order: &LimitOrder,
    fee_bps: u32,
    horizons_ns: &[u64],
) -> Result<MarkoutReport, ProbeError> {
    let mut collector = TimedCollector::default();
    astra_replay::replay(input, seed, &mut collector)?;
    Ok(MarkoutReport {
        trades: collector.trades,
        gaps: collector.gaps,
        outcome: markout(order, fee_bps, &collector.events, horizons_ns)?,
    })
}

/// Parse `500ms,1s,10s,60s` style horizons into nanoseconds. Rejected
/// loudly: a silently rounded horizon would corrupt every markout it labels.
/// A bare number means seconds.
pub fn parse_horizons(spec: &str) -> Result<Vec<u64>, String> {
    const NS_PER_MS: u64 = 1_000_000;
    const NS_PER_S: u64 = 1_000_000_000;
    const NS_PER_M: u64 = 60 * NS_PER_S;
    const NS_PER_H: u64 = 3_600 * NS_PER_S;

    let mut horizons = Vec::new();
    for part in spec.split(',') {
        let part = part.trim();
        let (number, unit) = if let Some(number) = part.strip_suffix("ms") {
            (number, NS_PER_MS)
        } else if let Some(number) = part.strip_suffix('s') {
            (number, NS_PER_S)
        } else if let Some(number) = part.strip_suffix('m') {
            (number, NS_PER_M)
        } else if let Some(number) = part.strip_suffix('h') {
            (number, NS_PER_H)
        } else {
            (part, NS_PER_S)
        };
        let amount: u64 = number
            .trim()
            .parse()
            .map_err(|_| format!("bad horizon {part:?}: want like 500ms,1s,10s"))?;
        if amount == 0 {
            return Err(format!("bad horizon {part:?}: must be positive"));
        }
        horizons.push(
            amount
                .checked_mul(unit)
                .ok_or_else(|| format!("bad horizon {part:?}: overflows nanoseconds"))?,
        );
    }
    if horizons.is_empty() {
        return Err("bad horizons: empty list".to_owned());
    }
    Ok(horizons)
}

/// Render the markout section exactly as the CLI prints it when `--markout`
/// is passed. Golden-tested beside the probe report; the default probe
/// output is byte-identical with or without this existing.
pub fn format_markout_report(report: &MarkoutReport, horizons_ns: &[u64]) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    match &report.outcome {
        MarkoutOutcome::Filled { markouts_bps, .. } => {
            for (horizon, bps) in horizons_ns.iter().zip(markouts_bps.iter()) {
                match bps {
                    Some(value) => {
                        let _ = writeln!(out, "markout_{horizon}ns {value:.2}");
                    }
                    None => {
                        let _ = writeln!(out, "markout_{horizon}ns unknown");
                    }
                }
            }
        }
        MarkoutOutcome::Unfilled { .. } => {
            let _ = writeln!(out, "markout     no-fill-no-markout");
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn price(s: &str) -> Fixed {
        s.parse().unwrap()
    }

    fn buy_at(price: &str) -> LimitOrder {
        LimitOrder {
            side: Side::Buy,
            price: price.parse().unwrap(),
            quantity: "1".parse().unwrap(),
        }
    }

    fn sell_at(price: &str) -> LimitOrder {
        LimitOrder {
            side: Side::Sell,
            price: price.parse().unwrap(),
            quantity: "1".parse().unwrap(),
        }
    }

    fn prints(prices: &[&str]) -> Vec<MarketEvent> {
        prices
            .iter()
            .enumerate()
            .map(|(index, price)| {
                MarketEvent::Print(Print {
                    seq: index as u64,
                    price: price.parse().unwrap(),
                    quantity: Fixed::ONE,
                })
            })
            .collect()
    }

    #[test]
    fn a_buy_fills_on_the_first_print_at_or_through() {
        let events = prints(&["101.00000000", "100.00000000", "99.00000000"]);

        let outcome = simulate(&buy_at("100.00000000"), 5, &events).unwrap();

        assert_eq!(
            outcome,
            OrderOutcome::Filled {
                fill_price: price("100.00000000"),
                fee: price("0.05000000"),
                slippage: Fixed::ZERO,
                print_seq: 1,
            }
        );
    }

    #[test]
    fn a_sell_fills_from_below_not_from_above() {
        let events = prints(&["99.00000000", "100.00000000", "101.00000000"]);

        let outcome = simulate(&sell_at("100.00000000"), 5, &events).unwrap();

        assert_eq!(
            outcome,
            OrderOutcome::Filled {
                fill_price: price("100.00000000"),
                fee: price("0.05000000"),
                slippage: Fixed::ZERO,
                print_seq: 1,
            }
        );
    }

    #[test]
    fn quotes_without_prints_are_not_in_the_input() {
        // The model only ever sees prints: a stream that never trades
        // through expires unfilled, however close it came.
        let events = prints(&["100.00000001", "100.00000001"]);

        let outcome = simulate(&buy_at("100.00000000"), 5, &events).unwrap();

        assert_eq!(
            outcome,
            OrderOutcome::Unfilled {
                reason: UnfilledReason::NoThroughPrint
            }
        );
    }

    #[test]
    fn corrupt_prints_are_skipped_never_filled() {
        // Hostile prints at non-positive prices or quantities would "trade
        // through" any limit. They must be skipped: fills need real
        // counterparties trading real size.
        let qty = |raw: i128| Fixed::from_raw(raw);
        let events = vec![
            MarketEvent::Print(Print {
                seq: 0,
                price: Fixed::from_raw(-5),
                quantity: qty(5),
            }),
            MarketEvent::Print(Print {
                seq: 1,
                price: Fixed::ZERO,
                quantity: qty(5),
            }),
            MarketEvent::Print(Print {
                seq: 2,
                price: price("50.00000000"),
                quantity: Fixed::ZERO,
            }),
            MarketEvent::Print(Print {
                seq: 3,
                price: price("50.00000000"),
                quantity: qty(-5),
            }),
        ];

        assert_eq!(
            simulate(&buy_at("100.00000000"), 5, &events).unwrap(),
            OrderOutcome::Unfilled {
                reason: UnfilledReason::NoThroughPrint
            }
        );
    }

    #[test]
    fn a_wrong_side_print_never_fills() {
        let events = prints(&["101.00000000", "102.00000000"]);

        assert_eq!(
            simulate(&buy_at("100.00000000"), 5, &events).unwrap(),
            OrderOutcome::Unfilled {
                reason: UnfilledReason::NoThroughPrint
            }
        );
    }

    #[test]
    fn a_gap_voids_even_when_a_later_print_would_fill() {
        let qty = Fixed::ONE;
        let events = vec![
            MarketEvent::Print(Print {
                seq: 0,
                price: price("101.00000000"),
                quantity: qty,
            }),
            MarketEvent::Gap,
            MarketEvent::Print(Print {
                seq: 2,
                price: price("99.00000000"),
                quantity: qty,
            }),
        ];

        assert_eq!(
            simulate(&buy_at("100.00000000"), 5, &events).unwrap(),
            OrderOutcome::Unfilled {
                reason: UnfilledReason::VoidedByGap
            }
        );
    }

    #[test]
    fn an_empty_stream_expires_unfilled() {
        assert_eq!(
            simulate(&buy_at("100.00000000"), 5, &[]).unwrap(),
            OrderOutcome::Unfilled {
                reason: UnfilledReason::NoThroughPrint
            }
        );
    }

    #[test]
    fn fees_are_exact_to_the_raw_unit() {
        // 200.00000000 notional at 5bps = 0.10000000 exactly.
        let order = LimitOrder {
            side: Side::Buy,
            price: price("100.00000000"),
            quantity: price("2"),
        };
        let events = prints(&["100.00000000"]);

        let OrderOutcome::Filled { fee, .. } = simulate(&order, 5, &events).unwrap() else {
            panic!("expected a fill");
        };
        assert_eq!(fee.to_string(), "0.10000000");

        // Half-away-from-zero at the raw unit: 1 raw * 5000bps / 10000 = 0.5 -> 1.
        assert_eq!(
            maker_fee(Fixed::from_raw(1), 5_000).unwrap(),
            Fixed::from_raw(1)
        );
        // Just below half rounds down: 1 raw * 4999bps / 10000 = 0.4999 -> 0.
        assert_eq!(
            maker_fee(Fixed::from_raw(1), 4_999).unwrap(),
            Fixed::from_raw(0)
        );
    }

    #[test]
    fn zero_fee_tier_is_allowed_but_explicit() {
        let events = prints(&["99.00000000"]);

        let OrderOutcome::Filled { fee, .. } =
            simulate(&buy_at("100.00000000"), 0, &events).unwrap()
        else {
            panic!("expected a fill");
        };
        assert_eq!(fee, Fixed::ZERO);
    }

    #[test]
    fn arithmetic_overflow_fails_loudly() {
        // Absurd size at max fee tier overflows fixed-point math: the error
        // must name overflow, never wrap into a cheerful tiny fee.
        let events = prints(&["1000000.00000000"]);
        let order = LimitOrder {
            side: Side::Buy,
            price: "1000000.00000000".parse().unwrap(),
            quantity: "100000000000000000000.00000000".parse().unwrap(),
        };
        assert!(matches!(
            simulate(&order, u32::MAX, &events),
            Err(ExecError::ArithmeticOverflow)
        ));
    }

    #[test]
    fn non_positive_orders_are_rejected_not_simulated() {
        let events = prints(&["1.00000000"]);

        for (side, bad_price, bad_quantity) in [
            (Side::Buy, "0.00000000", "1"),
            (Side::Buy, "-1.00000000", "1"),
            (Side::Sell, "1.00000000", "0"),
            (Side::Sell, "1.00000000", "-2"),
        ] {
            let order = LimitOrder {
                side,
                price: bad_price.parse().unwrap(),
                quantity: bad_quantity.parse().unwrap(),
            };
            assert!(
                matches!(
                    simulate(&order, 5, &events),
                    Err(ExecError::InvalidOrder(_))
                ),
                "order {bad_price} x {bad_quantity} should be rejected"
            );
        }
    }

    #[test]
    fn the_probe_report_format_is_pinned_line_by_line() {
        let order = buy_at("100.00000000");
        let filled = ProbeReport {
            trades: 2,
            gaps: 0,
            outcome: OrderOutcome::Filled {
                fill_price: price("100.00000000"),
                fee: price("0.05000000"),
                slippage: Fixed::ZERO,
                print_seq: 1,
            },
        };
        let text =
            format_probe_report(std::path::Path::new("./capture"), "buy", &order, 5, &filled);
        for line in [
            "input       ./capture",
            "order       buy 100.00000000 x 1.00000000",
            "fee_bps     5",
            "trades      2",
            "gaps        0",
            "result      filled",
            "fill_price  100.00000000",
            "fee         0.05000000",
            "slippage    0.00000000",
            "print_seq   1",
            "model       exec-v1",
        ] {
            assert!(text.contains(line), "missing line: {line}\n{text}");
        }

        let unfilled = ProbeReport {
            trades: 0,
            gaps: 1,
            outcome: OrderOutcome::Unfilled {
                reason: UnfilledReason::VoidedByGap,
            },
        };
        let text = format_probe_report(
            std::path::Path::new("./capture"),
            "sell",
            &sell_at("100.00000000"),
            5,
            &unfilled,
        );
        assert!(
            text.contains("result      unfilled (voided-by-gap)"),
            "{text}"
        );
    }

    const T0: i64 = 1_700_000_000_000_000_000;
    const SECOND: u64 = 1_000_000_000;

    fn timed(seq: u64, px: &str, at_ns: i64) -> TimedEvent {
        TimedEvent::Print(TimedPrint {
            seq,
            price: px.parse().unwrap(),
            quantity: price("1"),
            ts_socket_ns: at_ns,
        })
    }

    fn filled_markouts(events: &[TimedEvent], horizons: &[u64]) -> Vec<Option<f64>> {
        match markout(&buy_at("100"), 5, events, horizons).unwrap() {
            MarkoutOutcome::Filled { markouts_bps, .. } => markouts_bps,
            MarkoutOutcome::Unfilled { reason } => panic!("expected a fill, got {reason:?}"),
        }
    }

    /// Floats are analytics: compare within a tight epsilon, never exactly.
    fn assert_bps(actual: Vec<Option<f64>>, expected: Vec<Option<f64>>) {
        assert_eq!(actual.len(), expected.len(), "{actual:?} vs {expected:?}");
        for (got, want) in actual.into_iter().zip(expected) {
            match (got, want) {
                (Some(got), Some(want)) => {
                    assert!((got - want).abs() < 1e-6, "{got} vs {want}");
                }
                (None, None) => {}
                _ => panic!("{got:?} vs {want:?}"),
            }
        }
    }

    #[test]
    fn markouts_sign_adverse_selection_negative() {
        // Buy filled at 100: a fall to 99 is -100 bps adverse, a rise to
        // 101 is +100 bps benign. The sign carries the finding, not the leg.
        let events = vec![
            timed(0, "100", T0),
            timed(1, "99", T0 + SECOND as i64),
            timed(2, "101", T0 + 2 * SECOND as i64),
        ];
        assert_bps(
            filled_markouts(&events, &[SECOND, 2 * SECOND]),
            vec![Some(-100.0), Some(100.0)],
        );
    }

    #[test]
    fn sells_mirror_the_sign() {
        let events = vec![timed(0, "100", T0), timed(1, "101", T0 + SECOND as i64)];
        match markout(&sell_at("100"), 5, &events, &[SECOND]).unwrap() {
            MarkoutOutcome::Filled { markouts_bps, .. } => {
                assert_bps(markouts_bps, vec![Some(-100.0)]);
            }
            MarkoutOutcome::Unfilled { reason } => panic!("expected a fill, got {reason:?}"),
        }
    }

    #[test]
    fn gaps_and_missing_coverage_are_unknown_not_zero() {
        // A gap between fill and horizon voids it; a horizon no print
        // reaches stays unknown. Both are None — never a fake 0.00.
        let gapped = vec![
            timed(0, "100", T0),
            TimedEvent::Gap,
            timed(2, "50", T0 + 10 * SECOND as i64),
        ];
        assert_bps(filled_markouts(&gapped, &[SECOND]), vec![None]);

        let short = vec![timed(0, "100", T0)];
        assert_bps(filled_markouts(&short, &[SECOND]), vec![None]);
    }

    #[test]
    fn corrupt_cover_prints_are_skipped_like_in_fills() {
        let events = vec![
            timed(0, "100", T0),
            timed(1, "0", T0 + SECOND as i64),
            timed(2, "99", T0 + 2 * SECOND as i64),
        ];
        assert_bps(filled_markouts(&events, &[SECOND]), vec![Some(-100.0)]);
    }

    #[test]
    fn unfilled_orders_carry_their_reason_into_markouts() {
        let events = vec![timed(0, "101", T0)];
        assert!(matches!(
            markout(&buy_at("100"), 5, &events, &[SECOND]).unwrap(),
            MarkoutOutcome::Unfilled {
                reason: UnfilledReason::NoThroughPrint
            }
        ));
    }

    #[test]
    fn horizons_parse_strictly_or_not_at_all() {
        assert_eq!(
            parse_horizons("500ms,1s,10s").unwrap(),
            vec![500_000_000, 1_000_000_000, 10_000_000_000]
        );
        assert_eq!(parse_horizons("60").unwrap(), vec![60_000_000_000]);
        assert_eq!(parse_horizons("2m").unwrap(), vec![120_000_000_000]);
        assert_eq!(parse_horizons("1h").unwrap(), vec![3_600_000_000_000]);
        assert!(parse_horizons("0s").is_err());
        assert!(parse_horizons("").is_err());
        assert!(parse_horizons("soon").is_err());
        assert!(parse_horizons("1x").is_err());
        assert!(parse_horizons("18446744073709551615h").is_err());
    }

    #[test]
    fn the_markout_report_format_is_pinned_line_by_line() {
        let filled = MarkoutReport {
            trades: 3,
            gaps: 0,
            outcome: MarkoutOutcome::Filled {
                fill_price: price("100"),
                fee: price("0.05"),
                slippage: Fixed::ZERO,
                print_seq: 0,
                fill_ts_ns: T0,
                horizons_ns: vec![SECOND, 2 * SECOND],
                markouts_bps: vec![Some(-100.0), None],
            },
        };
        let text = format_markout_report(&filled, &[SECOND, 2 * SECOND]);
        for line in [
            "markout_1000000000ns -100.00",
            "markout_2000000000ns unknown",
        ] {
            assert!(text.contains(line), "missing line: {line}\n{text}");
        }

        let unfilled = MarkoutReport {
            trades: 0,
            gaps: 0,
            outcome: MarkoutOutcome::Unfilled {
                reason: UnfilledReason::NoThroughPrint,
            },
        };
        let text = format_markout_report(&unfilled, &[SECOND]);
        assert!(text.contains("markout     no-fill-no-markout"), "{text}");
    }
}
