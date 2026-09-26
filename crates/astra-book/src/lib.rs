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
}
