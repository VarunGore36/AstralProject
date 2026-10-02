use astra_book::Reconstructor;
use astra_types::GapMarker;

use crate::replay::{BookDiffEvent, Context, SnapshotEvent, Strategy};

pub struct BookTop {
    book: Reconstructor,
}

impl BookTop {
    pub fn new() -> Self {
        BookTop {
            book: Reconstructor::new(),
        }
    }

    pub fn book(&self) -> &Reconstructor {
        &self.book
    }
}

impl Default for BookTop {
    fn default() -> Self {
        Self::new()
    }
}

impl Strategy for BookTop {
    fn on_book_diff(&mut self, event: &BookDiffEvent, ctx: &mut Context) {
        let _ = self.book.apply_event(event.span, &event.diff);
        emit_top(&self.book, event.seq, ctx);
    }

    fn on_snapshot(&mut self, event: &SnapshotEvent, ctx: &mut Context) {
        if self.book.load_snapshot(&event.snapshot).is_ok() {
            emit_top(&self.book, event.seq, ctx);
        }
    }

    fn on_gap(&mut self, _marker: &GapMarker, _ctx: &mut Context) {}
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
    fn gaps_emit_nothing_but_do_not_break_the_strategy() {
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

        assert_eq!(ctx.signals().len(), 1);
    }
}
