use std::collections::BTreeMap;

use astra_types::Fixed;
use thiserror::Error;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Level {
    pub price: Fixed,
    pub quantity: Fixed,
}

#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct BookDiff {
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

impl BookDiff {
    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct UpdateSpan {
    pub first: u64,
    pub last: u64,
}

impl UpdateSpan {
    pub fn new(first: u64, last: u64) -> Self {
        UpdateSpan { first, last }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct BookSnapshot {
    pub last_update_id: u64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ApplyDecision {
    Applied,
    SkippedBeforeSnapshot,
    Discontinuity,
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum BookError {
    #[error("book update has a non-positive price: {0}")]
    InvalidPrice(Fixed),
    #[error("book update has a negative quantity: {0}")]
    InvalidQuantity(Fixed),
}

#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct OrderBook {
    bids: BTreeMap<Fixed, Fixed>,
    asks: BTreeMap<Fixed, Fixed>,
}

impl OrderBook {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn load_snapshot(&mut self, snapshot: &BookSnapshot) -> Result<(), BookError> {
        self.bids.clear();
        self.asks.clear();
        for level in &snapshot.bids {
            self.set_level(Side::Bid, level)?;
        }
        for level in &snapshot.asks {
            self.set_level(Side::Ask, level)?;
        }
        Ok(())
    }

    fn set_level(&mut self, side: Side, level: &Level) -> Result<(), BookError> {
        if level.price.raw() <= 0 {
            return Err(BookError::InvalidPrice(level.price));
        }
        if level.quantity.raw() <= 0 {
            return Err(BookError::InvalidQuantity(level.quantity));
        }

        self.book_for(side).insert(level.price, level.quantity);

        Ok(())
    }

    fn book_for(&mut self, side: Side) -> &mut BTreeMap<Fixed, Fixed> {
        match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        }
    }

    pub fn apply_diff(&mut self, diff: &BookDiff) -> Result<(), BookError> {
        for level in &diff.bids {
            self.apply_level(Side::Bid, level)?;
        }
        for level in &diff.asks {
            self.apply_level(Side::Ask, level)?;
        }
        Ok(())
    }

    fn apply_level(&mut self, side: Side, level: &Level) -> Result<(), BookError> {
        if level.price.raw() <= 0 {
            return Err(BookError::InvalidPrice(level.price));
        }
        if level.quantity.raw() < 0 {
            return Err(BookError::InvalidQuantity(level.quantity));
        }

        let book = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };

        if level.quantity.is_zero() {
            book.remove(&level.price);
        } else {
            book.insert(level.price, level.quantity);
        }

        Ok(())
    }

    pub fn best(&self, side: Side) -> Option<Level> {
        let entry = match side {
            Side::Bid => self.bids.iter().next_back(),
            Side::Ask => self.asks.iter().next(),
        };
        entry.map(|(price, quantity)| Level {
            price: *price,
            quantity: *quantity,
        })
    }

    pub fn best_bid(&self) -> Option<Level> {
        self.best(Side::Bid)
    }

    pub fn best_ask(&self) -> Option<Level> {
        self.best(Side::Ask)
    }

    pub fn levels(&self, side: Side, limit: usize) -> Vec<Level> {
        let collect = |iterator: Box<dyn Iterator<Item = (&Fixed, &Fixed)>>| {
            iterator
                .take(limit)
                .map(|(price, quantity)| Level {
                    price: *price,
                    quantity: *quantity,
                })
                .collect::<Vec<Level>>()
        };

        match side {
            Side::Bid => collect(Box::new(self.bids.iter().rev())),
            Side::Ask => collect(Box::new(self.asks.iter())),
        }
    }

    pub fn mid(&self) -> Option<Fixed> {
        let bid = self.best_bid()?;
        let ask = self.best_ask()?;
        bid.price
            .checked_add(ask.price)?
            .checked_div(Fixed::from_units(2))
    }

    pub fn spread(&self) -> Option<Fixed> {
        let bid = self.best_bid()?;
        let ask = self.best_ask()?;
        ask.price.checked_sub(bid.price)
    }

    pub fn is_crossed(&self) -> bool {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => bid.price >= ask.price,
            _ => false,
        }
    }

    pub fn bids_len(&self) -> usize {
        self.bids.len()
    }

    pub fn asks_len(&self) -> usize {
        self.asks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Reconstructor {
    book: OrderBook,
    snapshot_id: Option<u64>,
    last_update_id: Option<u64>,
    broken: bool,
    pub applied: u64,
    pub skipped_before_snapshot: u64,
    pub gaps: u64,
    pub rejected_after_gap: u64,
}

impl Reconstructor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn load_snapshot(&mut self, snapshot: &BookSnapshot) -> Result<(), BookError> {
        self.book.load_snapshot(snapshot)?;
        self.snapshot_id = Some(snapshot.last_update_id);
        self.last_update_id = Some(snapshot.last_update_id);
        self.broken = false;
        Ok(())
    }

    pub fn apply_event(
        &mut self,
        span: UpdateSpan,
        diff: &BookDiff,
    ) -> Result<ApplyDecision, BookError> {
        if self.broken {
            self.rejected_after_gap += 1;
            return Ok(ApplyDecision::Discontinuity);
        }

        if let Some(snapshot_id) = self.snapshot_id {
            if span.last <= snapshot_id {
                self.skipped_before_snapshot += 1;
                return Ok(ApplyDecision::SkippedBeforeSnapshot);
            }
        }

        if let Some(previous) = self.last_update_id {
            if span.first > previous + 1 {
                self.broken = true;
                self.gaps += 1;
                return Ok(ApplyDecision::Discontinuity);
            }
        }

        self.book.apply_diff(diff)?;
        self.last_update_id = Some(span.last);
        self.applied += 1;

        Ok(ApplyDecision::Applied)
    }

    pub fn book(&self) -> &OrderBook {
        &self.book
    }

    pub fn last_update_id(&self) -> Option<u64> {
        self.last_update_id
    }

    pub fn is_reliable(&self) -> bool {
        !self.broken
    }
}

pub fn compare_top_levels(
    book: &OrderBook,
    reference: &BookSnapshot,
    levels: usize,
) -> Vec<String> {
    let mut mismatches = Vec::new();
    compare_side(Side::Bid, book, &reference.bids, levels, &mut mismatches);
    compare_side(Side::Ask, book, &reference.asks, levels, &mut mismatches);
    mismatches
}

fn compare_side(
    side: Side,
    book: &OrderBook,
    reference: &[Level],
    levels: usize,
    mismatches: &mut Vec<String>,
) {
    let mut expected = reference.to_vec();
    match side {
        Side::Bid => expected.sort_by_key(|level| std::cmp::Reverse(level.price)),
        Side::Ask => expected.sort_by_key(|level| level.price),
    };
    expected.truncate(levels);

    let actual = book.levels(side, levels);
    let label = match side {
        Side::Bid => "bid",
        Side::Ask => "ask",
    };

    if actual.len() != expected.len() {
        mismatches.push(format!(
            "{label} depth: book has {} levels, reference has {}",
            actual.len(),
            expected.len()
        ));
    }

    for (index, (found, wanted)) in actual.iter().zip(expected.iter()).enumerate() {
        if found != wanted {
            mismatches.push(format!(
                "{label} level {index}: book has {} x {}, reference has {} x {}",
                found.price, found.quantity, wanted.price, wanted.quantity
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn level(price: &str, quantity: &str) -> Level {
        Level {
            price: price.parse().unwrap(),
            quantity: quantity.parse().unwrap(),
        }
    }

    fn diff(bids: Vec<Level>, asks: Vec<Level>) -> BookDiff {
        BookDiff { bids, asks }
    }

    fn book() -> OrderBook {
        let mut book = OrderBook::new();
        book.apply_diff(&diff(
            vec![level("100.00000000", "1"), level("99.00000000", "2")],
            vec![level("101.00000000", "1"), level("102.00000000", "3")],
        ))
        .unwrap();
        book
    }

    #[test]
    fn a_diff_establishes_both_sides() {
        let book = book();
        assert_eq!(book.best_bid().unwrap(), level("100.00000000", "1"));
        assert_eq!(book.best_ask().unwrap(), level("101.00000000", "1"));
        assert_eq!(book.bids_len(), 2);
        assert_eq!(book.asks_len(), 2);
        assert!(!book.is_crossed());
    }

    #[test]
    fn zero_quantity_removes_a_level() {
        let mut book = book();
        book.apply_diff(&diff(vec![level("100.00000000", "0")], vec![]))
            .unwrap();

        assert_eq!(book.best_bid().unwrap(), level("99.00000000", "2"));
        assert_eq!(book.bids_len(), 1);
    }

    #[test]
    fn a_new_quantity_replaces_the_old_one() {
        let mut book = book();
        book.apply_diff(&diff(vec![level("100.00000000", "7.5")], vec![]))
            .unwrap();

        assert_eq!(book.best_bid().unwrap(), level("100.00000000", "7.5"));
        assert_eq!(book.bids_len(), 2);
    }

    #[test]
    fn levels_are_returned_best_first() {
        let book = book();
        assert_eq!(
            book.levels(Side::Bid, 2),
            vec![level("100.00000000", "1"), level("99.00000000", "2")]
        );
        assert_eq!(book.levels(Side::Ask, 1), vec![level("101.00000000", "1")]);
    }

    #[test]
    fn mid_and_spread_use_both_sides() {
        let book = book();
        assert_eq!(book.mid().unwrap().to_string(), "100.50000000");
        assert_eq!(book.spread().unwrap().to_string(), "1.00000000");
    }

    #[test]
    fn a_crossed_book_is_reported() {
        let mut book = book();
        assert!(!book.is_crossed());

        book.apply_diff(&diff(vec![level("105.00000000", "1")], vec![]))
            .unwrap();
        assert!(book.is_crossed());
    }

    #[test]
    fn applying_a_diff_twice_is_idempotent() {
        let mut once = book();
        let mut twice = book();
        let update = diff(
            vec![level("98.00000000", "4")],
            vec![level("103.00000000", "0")],
        );

        once.apply_diff(&update).unwrap();
        twice.apply_diff(&update).unwrap();
        twice.apply_diff(&update).unwrap();

        assert_eq!(once, twice);
    }

    #[test]
    fn an_empty_diff_changes_nothing() {
        let mut book = book();
        let before = book.clone();
        book.apply_diff(&BookDiff::default()).unwrap();
        assert_eq!(book, before);
    }

    #[test]
    fn invalid_levels_are_refused() {
        let mut book = book();
        assert_eq!(
            book.apply_diff(&diff(vec![level("0.00000000", "1")], vec![])),
            Err(BookError::InvalidPrice("0.00000000".parse().unwrap()))
        );
        assert_eq!(
            book.apply_diff(&diff(vec![], vec![level("1.00000000", "-1")])),
            Err(BookError::InvalidQuantity("-1.00000000".parse().unwrap()))
        );
    }

    #[test]
    fn removing_an_absent_level_is_harmless() {
        let mut book = book();
        book.apply_diff(&diff(vec![level("5.00000000", "0")], vec![]))
            .unwrap();
        assert_eq!(book.bids_len(), 2);
    }

    fn snapshot(last_update_id: u64) -> BookSnapshot {
        BookSnapshot {
            last_update_id,
            bids: vec![level("100.00000000", "1"), level("99.00000000", "2")],
            asks: vec![level("101.00000000", "1"), level("102.00000000", "3")],
        }
    }

    fn span(first: u64, last: u64) -> UpdateSpan {
        UpdateSpan::new(first, last)
    }

    #[test]
    fn a_snapshot_replaces_the_whole_book() {
        let mut book = book();
        book.load_snapshot(&snapshot(50)).unwrap();

        assert_eq!(book.best_bid().unwrap(), level("100.00000000", "1"));
        assert_eq!(book.bids_len(), 2);
        assert_eq!(book.asks_len(), 2);
    }

    #[test]
    fn a_snapshot_refuses_zero_quantities() {
        let mut bad = snapshot(50);
        bad.bids.push(level("98.00000000", "0"));

        let mut book = OrderBook::new();
        assert!(matches!(
            book.load_snapshot(&bad),
            Err(BookError::InvalidQuantity(_))
        ));
    }

    #[test]
    fn events_before_the_snapshot_are_skipped() {
        let mut reconstructor = Reconstructor::new();
        reconstructor.load_snapshot(&snapshot(100)).unwrap();

        let decision = reconstructor
            .apply_event(span(10, 20), &diff(vec![level("1.00000000", "1")], vec![]))
            .unwrap();

        assert_eq!(decision, ApplyDecision::SkippedBeforeSnapshot);
        assert_eq!(reconstructor.skipped_before_snapshot, 1);
        assert_eq!(reconstructor.applied, 0);
    }

    #[test]
    fn an_event_ending_exactly_at_the_snapshot_is_skipped() {
        let mut reconstructor = Reconstructor::new();
        reconstructor.load_snapshot(&snapshot(100)).unwrap();

        let decision = reconstructor
            .apply_event(span(90, 100), &diff(vec![level("1.00000000", "1")], vec![]))
            .unwrap();

        assert_eq!(decision, ApplyDecision::SkippedBeforeSnapshot);
        assert_eq!(reconstructor.applied, 0);
    }

    #[test]
    fn contiguous_events_after_a_snapshot_apply() {
        let mut reconstructor = Reconstructor::new();
        reconstructor.load_snapshot(&snapshot(100)).unwrap();

        let decisions = [
            reconstructor
                .apply_event(
                    span(95, 101),
                    &diff(vec![level("98.00000000", "4")], vec![]),
                )
                .unwrap(),
            reconstructor
                .apply_event(
                    span(102, 110),
                    &diff(vec![], vec![level("103.00000000", "1")]),
                )
                .unwrap(),
        ];

        assert_eq!(decisions, [ApplyDecision::Applied, ApplyDecision::Applied]);
        assert_eq!(reconstructor.applied, 2);
        assert!(reconstructor.is_reliable());
        assert_eq!(reconstructor.last_update_id(), Some(110));
    }

    #[test]
    fn a_gap_breaks_the_book_until_a_new_snapshot() {
        let mut reconstructor = Reconstructor::new();
        reconstructor.load_snapshot(&snapshot(100)).unwrap();
        reconstructor
            .apply_event(
                span(95, 101),
                &diff(vec![level("98.00000000", "4")], vec![]),
            )
            .unwrap();

        let decision = reconstructor
            .apply_event(
                span(200, 210),
                &diff(vec![level("1.00000000", "1")], vec![]),
            )
            .unwrap();
        assert_eq!(decision, ApplyDecision::Discontinuity);
        assert!(!reconstructor.is_reliable());
        assert_eq!(reconstructor.gaps, 1);

        let after_gap = reconstructor
            .apply_event(
                span(211, 220),
                &diff(vec![level("1.00000000", "1")], vec![]),
            )
            .unwrap();
        assert_eq!(after_gap, ApplyDecision::Discontinuity);
        assert_eq!(reconstructor.gaps, 1);
        assert_eq!(reconstructor.rejected_after_gap, 1);
        assert_eq!(reconstructor.applied, 1);

        reconstructor.load_snapshot(&snapshot(300)).unwrap();
        assert!(reconstructor.is_reliable());
    }

    #[test]
    fn a_book_built_without_a_snapshot_still_checks_continuity() {
        let mut reconstructor = Reconstructor::new();
        reconstructor
            .apply_event(span(1, 10), &diff(vec![level("98.00000000", "4")], vec![]))
            .unwrap();

        let decision = reconstructor
            .apply_event(span(50, 60), &diff(vec![level("97.00000000", "1")], vec![]))
            .unwrap();

        assert_eq!(decision, ApplyDecision::Discontinuity);
        assert_eq!(reconstructor.applied, 1);
    }
}
