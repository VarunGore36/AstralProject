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

use astra_types::Fixed;
use thiserror::Error;

/// Model version stamped on every result. Bump on any rule change.
pub const MODEL_VERSION: &str = "exec-v1";

/// Fee denominator: basis points.
const BPS_DENOMINATOR: i128 = 10_000;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct LimitOrder {
    pub side: Side,
    pub price: Fixed,
    pub quantity: Fixed,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Print {
    pub seq: u64,
    pub price: Fixed,
}

/// Replay output in stream order. Prints come from normalized trade rows;
/// gaps come from synthetic markers. Anything else never reaches this model.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MarketEvent {
    Print(Print),
    Gap,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum UnfilledReason {
    /// Stream ended (or input was empty) with no through-print.
    NoThroughPrint,
    /// A gap marker arrived while the order was still live.
    VoidedByGap,
}

#[derive(Clone, PartialEq, Eq, Debug)]
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
    #[error("fee arithmetic overflowed")]
    FeeOverflow,
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
                let through = match order.side {
                    Side::Buy => print.price.raw() <= order.price.raw(),
                    Side::Sell => print.price.raw() >= order.price.raw(),
                };
                if through {
                    let notional = order
                        .price
                        .checked_mul(order.quantity)
                        .ok_or(ExecError::FeeOverflow)?;
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
        .ok_or(ExecError::FeeOverflow)?;
    let quotient = scaled / BPS_DENOMINATOR;
    let bump = i128::from((scaled % BPS_DENOMINATOR).abs() >= BPS_DENOMINATOR / 2);
    let raw = quotient.checked_add(bump).ok_or(ExecError::FeeOverflow)?;
    Ok(Fixed::from_raw(raw))
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
        let events = vec![
            MarketEvent::Print(Print {
                seq: 0,
                price: price("101.00000000"),
            }),
            MarketEvent::Gap,
            MarketEvent::Print(Print {
                seq: 2,
                price: price("99.00000000"),
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
}
