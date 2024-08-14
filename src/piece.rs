use std::cmp;

#[derive(PartialEq, Debug, Clone)]
pub struct Piece {
    length: u64,
    fragments: Vec<(u64, u64)>, // begin, end
}

impl Piece {
    pub fn new(length: u64) -> Piece {
        Piece {
            length,
            fragments: Vec::new(),
        }
    }

    pub fn contains(&self, begin: u64, end: u64) -> bool {
        assert!(begin < end || end < self.length);
        match self.get_fragment_idx_containing_value(begin) {
            None => {
                return false;
            }
            Some(idx) => {
                if end <= self.fragments[idx].1 {
                    return true;
                }
                return false;
            }
        }
    }

    pub fn add_fragment(&mut self, begin: u64, end: u64) {
        assert!(begin < end || end < self.length);
        let closest_idx = self.get_closest_fragment_idx_containing_value(begin);
        let mut idx = closest_idx + 1;
        if begin < self.fragments[closest_idx].0 {
            idx = closest_idx;
        }
        if idx >= self.fragments.len() {
            self.fragments.push((begin, end));
        } else {
            self.fragments.insert(idx, (begin, end));
        }
        while idx + 1 < self.fragments.len() && end + 1 >= self.fragments[idx + 1].0 {
            self.fragments[idx] = (begin, cmp::max(end, self.fragments[idx + 1].1));
            self.fragments.remove(idx + 1);
        }
        if idx != 0 {
            if begin <= self.fragments[idx - 1].1 + 1 {
                self.fragments[idx] = (
                    cmp::min(begin, self.fragments[idx - 1].0),
                    self.fragments[idx].1,
                );
                self.fragments.remove(idx - 1);
            }
        }
    }

    pub fn complete(&self) -> bool {
        if self.length == 0
            || (self.fragments.len() == 1
                && self.fragments[0].0 == 0
                && self.fragments[0].1 == self.length - 1)
        {
            return true;
        }
        false
    }

    pub fn get_next_fragment(&self, max_fragment_size: u64) -> Option<(u64, u64)> {
        assert!(max_fragment_size > 0);
        if self.complete() {
            return None;
        } else if self.fragments.len() == 0 {
            let begin = 0;
            let end = cmp::min(max_fragment_size - 1, self.length - 1);
            return Some((begin, end));
        } else if self.fragments.len() == 1 {
            if self.fragments[0].0 == 0 {
                let begin = self.fragments[0].1 + 1;
                let end = cmp::min(begin + max_fragment_size - 1, self.length - 1);
                return Some((begin, end));
            } else {
                let begin = 0;
                let end = cmp::min(max_fragment_size - 1, self.fragments[0].0 - 1);
                return Some((begin, end));
            }
        } else {
            if self.fragments[0].0 == 0 {
                let begin = self.fragments[0].1 + 1;
                let end_with_max_fragment_size = begin + max_fragment_size - 1;
                let end_with_next_fragment = begin + self.fragments[1].0 - 1;
                let end = cmp::min(end_with_max_fragment_size, end_with_next_fragment);
                return Some((begin, end));
            } else {
                let begin = 0;
                let end = cmp::min(max_fragment_size - 1, self.fragments[0].0 - 1);
                return Some((begin, end));
            }
        }
    }

    // find the index of the fragment (between fragment i and j positions) that contains the value
    fn get_fragment_dx_containing_value_in_slice(
        &self,
        value: u64,
        i: usize,
        j: usize,
    ) -> Option<usize> {
        let mid = i + (j - i) / 2;
        if self.fragments[mid].0 <= value && value <= self.fragments[mid].1 {
            return Some(mid);
        } else if j == i {
            return None;
        } else if self.fragments[mid].0 > value {
            return self.get_fragment_dx_containing_value_in_slice(value, i, mid - 1);
        } else {
            return self.get_fragment_dx_containing_value_in_slice(value, mid + 1, j);
        }
    }

    fn get_fragment_idx_containing_value(&self, value: u64) -> Option<usize> {
        if self.fragments.len() == 0 {
            return None;
        }
        self.get_fragment_dx_containing_value_in_slice(value, 0, self.fragments.len() - 1)
    }

    // closest lower
    fn get_closest_fragment_idx_containing_value_in_slice(
        &self,
        value: u64,
        i: usize,
        j: usize,
    ) -> usize {
        let mid = i + (j - i) / 2;
        if self.fragments[mid].0 <= value && value <= self.fragments[mid].1 {
            return mid;
        } else if j == i {
            if j == 0 {
                return j;
            } else if value < self.fragments[j].0 {
                return j - 1;
            }
            return j;
        } else if self.fragments[mid].0 > value {
            return self.get_closest_fragment_idx_containing_value_in_slice(value, i, mid - 1);
        } else {
            return self.get_closest_fragment_idx_containing_value_in_slice(value, mid + 1, j);
        }
    }

    fn get_closest_fragment_idx_containing_value(&self, value: u64) -> usize {
        if self.fragments.len() == 0 {
            return 0;
        }
        self.get_closest_fragment_idx_containing_value_in_slice(value, 0, self.fragments.len() - 1)
    }
}

#[cfg(test)]
mod tests {
    use super::Piece;

    #[test]
    fn test_complete() {
        let piece = Piece {
            length: 10,
            fragments: vec![(0, 1), (3, 6), (8, 9)],
        };

        assert_eq!(piece.complete(), false);

        let piece = Piece {
            length: 10,
            fragments: vec![(0, 9)],
        };

        assert_eq!(piece.complete(), true);
    }

    #[test]
    fn test_get_fragment_idx_containing_value() {
        let piece = Piece {
            length: 0,
            fragments: vec![],
        };
        assert_eq!(piece.get_fragment_idx_containing_value(4), None);

        let piece = Piece {
            length: 20,
            fragments: vec![(0, 1), (3, 6), (8, 9), (15, 19)],
        };
        assert_eq!(piece.get_fragment_idx_containing_value(4), Some(1));

        let piece = Piece {
            length: 20,
            fragments: vec![(0, 1), (3, 6), (8, 9), (15, 19)],
        };
        assert_eq!(piece.get_fragment_idx_containing_value(9), Some(2));

        let piece = Piece {
            length: 20,
            fragments: vec![(0, 1), (3, 6), (8, 9), (15, 19)],
        };
        assert_eq!(piece.get_fragment_idx_containing_value(8), Some(2));

        let piece = Piece {
            length: 20,
            fragments: vec![(0, 1), (3, 6), (8, 9), (15, 19)],
        };
        assert_eq!(piece.get_fragment_idx_containing_value(6), Some(1));

        let piece = Piece {
            length: 20,
            fragments: vec![(0, 1), (3, 6), (8, 9), (15, 19)],
        };
        assert_eq!(piece.get_fragment_idx_containing_value(0), Some(0));

        let piece = Piece {
            length: 20,
            fragments: vec![(0, 1), (3, 6), (8, 9), (15, 19)],
        };
        assert_eq!(piece.get_fragment_idx_containing_value(2), None);

        let piece = Piece {
            length: 20,
            fragments: vec![(1, 2), (4, 6), (8, 9), (15, 19)],
        };
        assert_eq!(piece.get_fragment_idx_containing_value(0), None);

        let piece = Piece {
            length: 20,
            fragments: vec![(0, 1), (3, 6), (8, 9), (15, 19)],
        };
        assert_eq!(piece.get_fragment_idx_containing_value(10), None);

        let piece = Piece {
            length: 20,
            fragments: vec![(0, 1), (3, 6), (8, 9), (15, 19)],
        };
        assert_eq!(piece.get_fragment_idx_containing_value(14), None);

        let piece = Piece {
            length: 20,
            fragments: vec![(0, 1), (3, 6), (8, 9), (15, 19)],
        };
        assert_eq!(piece.get_fragment_idx_containing_value(15), Some(3));

        let piece = Piece {
            length: 20,
            fragments: vec![(0, 1), (3, 6), (8, 9), (15, 19)],
        };
        assert_eq!(piece.get_fragment_idx_containing_value(20), None);

        let piece = Piece {
            length: 20,
            fragments: vec![(0, 1), (3, 6), (8, 9), (15, 19)],
        };
        assert_eq!(piece.get_fragment_idx_containing_value(19), Some(3));
    }

    #[test]
    fn test_get_closest_fragment_idx_containing_value() {
        let piece = Piece {
            length: 0,
            fragments: vec![],
        };
        assert_eq!(piece.get_closest_fragment_idx_containing_value(4), 0);

        let piece = Piece {
            length: 10,
            fragments: vec![(0, 1), (3, 6), (8, 9)],
        };
        assert_eq!(piece.get_closest_fragment_idx_containing_value(4), 1);

        let piece = Piece {
            length: 10,
            fragments: vec![(0, 1), (3, 6), (8, 9)],
        };
        assert_eq!(piece.get_closest_fragment_idx_containing_value(9), 2);

        let piece = Piece {
            length: 10,
            fragments: vec![(0, 1), (3, 6), (8, 9)],
        };
        assert_eq!(piece.get_closest_fragment_idx_containing_value(8), 2);

        let piece = Piece {
            length: 10,
            fragments: vec![(0, 1), (3, 6), (8, 9)],
        };
        assert_eq!(piece.get_closest_fragment_idx_containing_value(6), 1);

        let piece = Piece {
            length: 10,
            fragments: vec![(0, 1), (3, 6), (8, 9)],
        };
        assert_eq!(piece.get_closest_fragment_idx_containing_value(0), 0);

        let piece = Piece {
            length: 10,
            fragments: vec![(0, 1), (3, 6), (8, 9)],
        };
        assert_eq!(piece.get_closest_fragment_idx_containing_value(2), 0);

        let piece = Piece {
            length: 10,
            fragments: vec![(1, 2), (4, 6), (8, 9)],
        };
        assert_eq!(piece.get_closest_fragment_idx_containing_value(0), 0);

        let piece = Piece {
            length: 10,
            fragments: vec![(0, 1), (3, 6), (8, 9)],
        };
        assert_eq!(piece.get_closest_fragment_idx_containing_value(10), 2);

        let piece = Piece {
            length: 10,
            fragments: vec![(0, 1), (3, 6), (8, 9)],
        };
        assert_eq!(piece.get_closest_fragment_idx_containing_value(7), 1);

        let piece = Piece {
            length: 30,
            fragments: vec![(0, 1), (3, 6), (8, 9), (11, 14), (18, 21), (24, 29)],
        };
        assert_eq!(piece.get_closest_fragment_idx_containing_value(7), 1);

        let piece = Piece {
            length: 30,
            fragments: vec![(0, 1), (3, 6), (8, 9), (11, 14), (18, 21), (24, 29)],
        };
        assert_eq!(piece.get_closest_fragment_idx_containing_value(23), 4);

        let piece = Piece {
            length: 30,
            fragments: vec![(0, 1), (3, 6), (8, 9), (11, 14), (18, 21), (24, 29)],
        };
        assert_eq!(piece.get_closest_fragment_idx_containing_value(2), 0);

        let piece = Piece {
            length: 30,
            fragments: vec![(0, 1), (3, 6), (8, 9), (11, 14), (18, 21), (24, 29)],
        };
        assert_eq!(piece.get_closest_fragment_idx_containing_value(7), 1);

        let piece = Piece {
            length: 30,
            fragments: vec![(0, 1), (3, 6), (8, 9), (11, 14), (18, 21), (24, 29)],
        };
        assert_eq!(piece.get_closest_fragment_idx_containing_value(10), 2);

        let piece = Piece {
            length: 30,
            fragments: vec![(0, 1), (3, 6), (8, 9), (11, 14), (18, 21), (24, 29)],
        };
        assert_eq!(piece.get_closest_fragment_idx_containing_value(15), 3);

        let piece = Piece {
            length: 10,
            fragments: vec![(0, 1), (3, 6), (8, 9)],
        };
        assert_eq!(piece.get_closest_fragment_idx_containing_value(2), 0);
    }

    #[test]
    fn test_add_fragment() {
        let mut piece = Piece {
            length: 30,
            fragments: vec![(0, 1), (3, 4), (8, 9), (11, 14), (18, 21), (24, 29)],
        };
        piece.add_fragment(5, 5);
        let expect = Piece {
            length: 30,
            fragments: vec![(0, 1), (3, 5), (8, 9), (11, 14), (18, 21), (24, 29)],
        };
        assert_eq!(piece, expect);

        let mut piece = Piece {
            length: 30,
            fragments: vec![(0, 1), (3, 4), (8, 9), (11, 14), (18, 21), (24, 29)],
        };
        piece.add_fragment(7, 7);
        let expect = Piece {
            length: 30,
            fragments: vec![(0, 1), (3, 4), (7, 9), (11, 14), (18, 21), (24, 29)],
        };
        assert_eq!(piece, expect);

        let mut piece = Piece {
            length: 30,
            fragments: vec![(0, 1), (3, 6), (8, 9), (11, 14), (18, 21), (24, 29)],
        };
        piece.add_fragment(10, 10);
        let expect = Piece {
            length: 30,
            fragments: vec![(0, 1), (3, 6), (8, 14), (18, 21), (24, 29)],
        };
        assert_eq!(piece, expect);

        let mut piece = Piece {
            length: 50,
            fragments: vec![(0, 1), (3, 6), (8, 9), (11, 14), (18, 21), (24, 29)],
        };
        piece.add_fragment(31, 35);
        let expect = Piece {
            length: 50,
            fragments: vec![
                (0, 1),
                (3, 6),
                (8, 9),
                (11, 14),
                (18, 21),
                (24, 29),
                (31, 35),
            ],
        };
        assert_eq!(piece, expect);

        let mut piece = Piece {
            length: 50,
            fragments: vec![(0, 1), (3, 6), (8, 9), (11, 14), (18, 21), (24, 29)],
        };
        piece.add_fragment(30, 35);
        let expect = Piece {
            length: 50,
            fragments: vec![(0, 1), (3, 6), (8, 9), (11, 14), (18, 21), (24, 35)],
        };
        assert_eq!(piece, expect);

        let mut piece = Piece {
            length: 50,
            fragments: vec![(0, 1), (3, 6), (8, 9), (11, 14), (18, 21), (24, 29)],
        };
        piece.add_fragment(11, 14);
        let expect = Piece {
            length: 50,
            fragments: vec![(0, 1), (3, 6), (8, 9), (11, 14), (18, 21), (24, 29)],
        };
        assert_eq!(piece, expect);

        let mut piece = Piece {
            length: 50,
            fragments: vec![(0, 1), (3, 6), (8, 9), (11, 14), (18, 21), (24, 29)],
        };
        piece.add_fragment(11, 21);
        let expect = Piece {
            length: 50,
            fragments: vec![(0, 1), (3, 6), (8, 9), (11, 21), (24, 29)],
        };
        assert_eq!(piece, expect);

        let mut piece = Piece {
            length: 50,
            fragments: vec![(0, 1), (3, 6), (8, 9), (11, 14), (18, 21), (24, 29)],
        };
        piece.add_fragment(7, 19);
        let expect = Piece {
            length: 50,
            fragments: vec![(0, 1), (3, 21), (24, 29)],
        };
        assert_eq!(piece, expect);

        let mut piece = Piece {
            length: 50,
            fragments: vec![(0, 1), (3, 6), (8, 9), (11, 14), (18, 21), (24, 29)],
        };
        piece.add_fragment(7, 31);
        let expect = Piece {
            length: 50,
            fragments: vec![(0, 1), (3, 31)],
        };
        assert_eq!(piece, expect);

        let mut piece = Piece {
            length: 50,
            fragments: vec![(0, 1), (3, 6), (8, 9), (11, 14), (18, 21), (24, 29)],
        };
        piece.add_fragment(0, 31);
        let expect = Piece {
            length: 50,
            fragments: vec![(0, 31)],
        };
        assert_eq!(piece, expect);

        let mut piece = Piece {
            length: 50,
            fragments: vec![(1, 29)],
        };
        piece.add_fragment(0, 31);
        let expect = Piece {
            length: 50,
            fragments: vec![(0, 31)],
        };
        assert_eq!(piece, expect);

        let mut piece = Piece {
            length: 50,
            fragments: vec![(1, 50)],
        };
        piece.add_fragment(0, 31);
        let expect = Piece {
            length: 50,
            fragments: vec![(0, 50)],
        };
        assert_eq!(piece, expect);

        let mut piece = Piece {
            length: 50,
            fragments: vec![(1, 40)],
        };
        piece.add_fragment(0, 45);
        let expect = Piece {
            length: 50,
            fragments: vec![(0, 45)],
        };
        assert_eq!(piece, expect);

        let mut piece = Piece {
            length: 50,
            fragments: vec![(1, 40)],
        };
        piece.add_fragment(1, 45);
        let expect = Piece {
            length: 50,
            fragments: vec![(1, 45)],
        };
        assert_eq!(piece, expect);

        let mut piece = Piece {
            length: 50,
            fragments: vec![(2, 40)],
        };
        piece.add_fragment(1, 40);
        let expect = Piece {
            length: 50,
            fragments: vec![(1, 40)],
        };
        assert_eq!(piece, expect);
    }

    #[test]
    fn test_next_fragment() {
        let piece = Piece {
            length: 50,
            fragments: vec![(0, 1), (3, 6), (8, 9), (11, 14), (18, 21), (24, 29)],
        };
        assert_eq!(piece.get_next_fragment(1), Some((2, 2)));

        let piece = Piece {
            length: 50,
            fragments: vec![(24, 29)],
        };
        assert_eq!(piece.get_next_fragment(5), Some((0, 4)));

        let piece = Piece {
            length: 50,
            fragments: vec![(24, 29)],
        };
        assert_eq!(piece.get_next_fragment(30), Some((0, 23)));

        let piece = Piece {
            length: 50,
            fragments: vec![(0, 29)],
        };
        assert_eq!(piece.get_next_fragment(30), Some((30, 49)));

        let piece = Piece {
            length: 50,
            fragments: vec![],
        };
        assert_eq!(piece.get_next_fragment(100), Some((0, 49)));

        let piece = Piece {
            length: 50,
            fragments: vec![],
        };
        assert_eq!(piece.get_next_fragment(30), Some((0, 29)));

        let piece = Piece {
            length: 50,
            fragments: vec![(1, 1), (3, 6), (8, 9), (11, 14), (18, 21), (24, 29)],
        };
        assert_eq!(piece.get_next_fragment(1), Some((0, 0)));
    }
}
