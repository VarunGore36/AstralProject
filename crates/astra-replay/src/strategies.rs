use astra_book::Reconstructor;
use astra_types::GapMarker;

use crate::replay::{BookDiffEvent, Context, SnapshotEvent, Strategy, TradeEvent};

pub struct BookTop {
    book: Reconstructor,
    broken: bool,
}

impl BookTop {
    pub fn new() -> Self {
        BookTop {
            book: Reconstructor::new(),
            broken: false,
        }
    }

    pub fn book(&self) -> &Reconstructor {
        &self.book
    }

    pub fn is_broken(&self) -> bool {
        self.broken
    }
}

impl Default for BookTop {
    fn default() -> Self {
        Self::new()
    }
}

impl Strategy for BookTop {
    fn on_book_diff(&mut self, event: &BookDiffEvent, ctx: &mut Context) {
        // A broken book emits nothing: tops built on a book the venue stream
        // already invalidated would measure a market that never existed. Only
        // a fresh snapshot heals it — same rule as the reconstructor itself.
        if self.broken {
            return;
        }
        let _ = self.book.apply_event(event.span, &event.diff);
        if !self.book.is_reliable() {
            self.broken = true;
            return;
        }
        emit_top(&self.book, event.seq, ctx);
    }

    fn on_snapshot(&mut self, event: &SnapshotEvent, ctx: &mut Context) {
        if self.book.load_snapshot(&event.snapshot).is_ok() {
            self.broken = false;
            emit_top(&self.book, event.seq, ctx);
        }
    }

    fn on_gap(&mut self, _marker: &GapMarker, _ctx: &mut Context) {
        self.broken = true;
    }
}

fn emit_top(book: &Reconstructor, seq: u64, ctx: &mut Context) {
    let book = book.book();
    let mut payload = Vec::with_capacity(41);
    payload.extend_from_slice(&seq.to_le_bytes());
    match book.best_bid() {
        Some(level) => {
            payload.push(1);
            payload.extend_from_slice(&level.price.raw().to_le_bytes());
            payload.extend_from_slice(&level.quantity.raw().to_le_bytes());
        }
        None => {
            payload.push(0);
            payload.extend_from_slice(&[0u8; 32]);
        }
    }
    match book.best_ask() {
        Some(level) => {
            payload.push(1);
            payload.extend_from_slice(&level.price.raw().to_le_bytes());
            payload.extend_from_slice(&level.quantity.raw().to_le_bytes());
        }
        None => {
            payload.push(0);
            payload.extend_from_slice(&[0u8; 32]);
        }
    }
    ctx.emit("top", payload);
}

/// Counts trade prints by side and emits one summary signal at stream end.
///
/// Deliberately gap-insensitive: prints observed are facts, and the tally
/// reports what arrived — voiding belongs to the fill model, not the
/// counter. Unknown sides (fail-closed `None` from the parsers) get their
/// own bucket rather than vanishing.
#[derive(Default)]
pub struct TradeTally {
    buys: u64,
    sells: u64,
    unknown: u64,
}

impl TradeTally {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn buys(&self) -> u64 {
        self.buys
    }

    pub fn sells(&self) -> u64 {
        self.sells
    }

    pub fn unknown(&self) -> u64 {
        self.unknown
    }
}

impl Strategy for TradeTally {
    fn on_book_diff(&mut self, _event: &BookDiffEvent, _ctx: &mut Context) {}
    fn on_snapshot(&mut self, _event: &SnapshotEvent, _ctx: &mut Context) {}

    fn on_trade(&mut self, event: &TradeEvent, _ctx: &mut Context) {
        match event.trade.side.as_deref() {
            Some("Buy") => self.buys += 1,
            Some("Sell") => self.sells += 1,
            _ => self.unknown += 1,
        }
    }

    fn on_gap(&mut self, _marker: &GapMarker, _ctx: &mut Context) {}

    fn on_end(&mut self, _frames: u64, ctx: &mut Context) {
        let mut payload = Vec::with_capacity(24);
        payload.extend_from_slice(&self.buys.to_le_bytes());
        payload.extend_from_slice(&self.sells.to_le_bytes());
        payload.extend_from_slice(&self.unknown.to_le_bytes());
        ctx.emit("tally", payload);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use astra_book::{BookDiff, Level};
    use astra_types::Timestamp;

    fn level(price: &str, quantity: &str) -> Level {
        Level {
            price: price.parse().unwrap(),
            quantity: quantity.parse().unwrap(),
        }
    }

    fn diff_event(
        seq: u64,
        first: u64,
        last: u64,
        bids: Vec<Level>,
        asks: Vec<Level>,
    ) -> BookDiffEvent {
        BookDiffEvent {
            seq,
            ts_socket: Timestamp::from_unix_nanos(seq as i64),
            ts_exchange: None,
            span: astra_book::UpdateSpan::new(first, last),
            diff: BookDiff { bids, asks },
        }
    }

    fn context() -> Context {
        Context::for_tests(1)
    }

    #[test]
    fn top_signals_track_the_book() {
        let mut strategy = BookTop::new();
        let mut ctx = context();

        strategy.on_book_diff(
            &diff_event(
                0,
                100,
                105,
                vec![level("100.00000000", "1")],
                vec![level("101.00000000", "1")],
            ),
            &mut ctx,
        );
        strategy.on_book_diff(
            &diff_event(1, 106, 110, vec![level("100.50000000", "2")], vec![]),
            &mut ctx,
        );

        assert_eq!(ctx.signals().len(), 2);
        assert_eq!(ctx.signals()[0].tag, "top");
        assert_eq!(
            strategy.book().book().best_bid().unwrap(),
            level("100.50000000", "2")
        );
    }

    #[test]
    fn gaps_break_the_strategy_until_a_snapshot_heals_it() {
        let mut strategy = BookTop::new();
        let mut ctx = context();

        strategy.on_book_diff(
            &diff_event(0, 100, 105, vec![level("100.00000000", "1")], vec![]),
            &mut ctx,
        );
        strategy.on_gap(
            &GapMarker {
                started_at: Timestamp::from_unix_nanos(0),
                ended_at: Timestamp::from_unix_nanos(1),
                attempts: 1,
                reason: "venue_close".to_owned(),
            },
            &mut ctx,
        );
        assert!(strategy.is_broken());

        // Post-gap diffs emit nothing: the book they would describe died
        // with the gap.
        strategy.on_book_diff(
            &diff_event(1, 106, 110, vec![level("100.50000000", "2")], vec![]),
            &mut ctx,
        );

        assert_eq!(ctx.signals().len(), 1);
    }

    #[test]
    fn a_snapshot_heals_a_broken_book_top() {
        use astra_book::BookSnapshot;

        let mut strategy = BookTop::new();
        let mut ctx = context();

        strategy.on_book_diff(
            &diff_event(0, 100, 105, vec![level("100.00000000", "1")], vec![]),
            &mut ctx,
        );
        strategy.on_gap(
            &GapMarker {
                started_at: Timestamp::from_unix_nanos(0),
                ended_at: Timestamp::from_unix_nanos(1),
                attempts: 1,
                reason: "venue_close".to_owned(),
            },
            &mut ctx,
        );
        assert!(strategy.is_broken());

        strategy.on_snapshot(
            &SnapshotEvent {
                seq: 2,
                ts_socket: Timestamp::from_unix_nanos(2),
                snapshot: BookSnapshot {
                    last_update_id: 200,
                    bids: vec![level("99.00000000", "5")],
                    asks: vec![level("101.00000000", "5")],
                },
            },
            &mut ctx,
        );

        assert!(!strategy.is_broken());
        assert_eq!(ctx.signals().len(), 2);
        assert_eq!(
            strategy.book().book().best_bid().unwrap(),
            level("99.00000000", "5")
        );
    }

    fn trade_event(seq: u64, side: Option<&str>) -> TradeEvent {
        TradeEvent {
            seq,
            print_index: 0,
            ts_socket: Timestamp::from_unix_nanos(seq as i64),
            ts_exchange: None,
            trade: astra_record::feed::TradePrint {
                trade_id: None,
                price: "1.00000000".parse().unwrap(),
                quantity: "1".parse().unwrap(),
                side: side.map(|side| side.to_owned()),
                ts_exchange: None,
            },
        }
    }

    #[test]
    fn tally_counts_sides_and_emits_once_at_end() {
        let mut strategy = TradeTally::new();
        let mut ctx = context();

        strategy.on_trade(&trade_event(0, Some("Buy")), &mut ctx);
        strategy.on_trade(&trade_event(1, Some("Sell")), &mut ctx);
        strategy.on_trade(&trade_event(2, Some("Buy")), &mut ctx);
        strategy.on_trade(&trade_event(3, None), &mut ctx);
        assert!(ctx.signals().is_empty());

        strategy.on_end(4, &mut ctx);

        assert_eq!(strategy.buys(), 2);
        assert_eq!(strategy.sells(), 1);
        assert_eq!(strategy.unknown(), 1);
        assert_eq!(ctx.signals().len(), 1);
        assert_eq!(ctx.signals()[0].tag, "tally");
    }
}
