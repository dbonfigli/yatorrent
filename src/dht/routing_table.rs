use std::str::FromStr;

use num_bigint::BigUint;

#[derive(PartialEq, Debug)]
pub struct Bucket {
    pub from: BigUint,
    pub to: BigUint,
    pub content: BucketContent,
    own_node_id: BigUint,
}

#[derive(PartialEq, Debug)]
pub enum BucketContent {
    Buckets(Vec<Bucket>), // max 2
    Nodes(Vec<BigUint>),  // max 8
}

static MAX_NODES_PER_BUCKET: usize = 8;

fn biguint_to_u8_20(n: &BigUint) -> [u8; 20] {
    let mut vec = n.to_bytes_be();
    while vec.len() < 20 {
        vec.insert(0, 0);
    }

    return vec.try_into().expect(
        format!(
            "could not convert biguint to [u8; 20] number is more than 20 bytes: {}",
            n
        )
        .as_str(),
    );
}

impl Bucket {
    pub fn new(own_node_id: [u8; 20]) -> Self {
        return Bucket {
            from: BigUint::from_str("0").unwrap(),
            to: BigUint::from_str("2").unwrap().pow(160) - BigUint::from_str("1").unwrap(),
            content: BucketContent::Nodes(Vec::new()),
            own_node_id: BigUint::from_bytes_be(&own_node_id),
        };
    }

    pub fn get(&self, node_id: &[u8; 20]) -> Option<[u8; 20]> {
        let node_id_big_uint = BigUint::from_bytes_be(node_id);
        match &self.content {
            BucketContent::Buckets(b) => {
                if node_id_big_uint <= b[0].to {
                    b[0].get(node_id)
                } else {
                    b[1].get(node_id)
                }
            }
            BucketContent::Nodes(n) => {
                if let Ok(i) = n.binary_search(&node_id_big_uint) {
                    Some(biguint_to_u8_20(&n[i]))
                } else {
                    None
                }
            }
        }
    }

    pub fn as_vec(&self) -> Vec<[u8; 20]> {
        self.as_vec_in_biguint()
            .iter()
            .map(|n| biguint_to_u8_20(n))
            .collect()
    }

    fn as_vec_in_biguint(&self) -> Vec<BigUint> {
        match &self.content {
            BucketContent::Buckets(b) => {
                let mut joined = b[0].as_vec_in_biguint();
                let mut right = b[1].as_vec_in_biguint();
                joined.append(&mut right);
                joined
            }
            BucketContent::Nodes(n) => n.clone(),
        }
    }

    pub fn closest_nodes(&self, node_id: &[u8; 20]) -> Vec<[u8; 20]> {
        let node_id_big_uint = BigUint::from_bytes_be(node_id);
        let ret_in_biguint = self.closest_nodes_in_biguint(&node_id_big_uint);
        ret_in_biguint.iter().map(|n| biguint_to_u8_20(n)).collect()
    }

    // todo: optimize this
    fn closest_nodes_in_biguint(&self, node_id: &BigUint) -> Vec<BigUint> {
        match &self.content {
            BucketContent::Buckets(b) => {
                let mut joined = b[0].closest_nodes_in_biguint(node_id);
                let mut right = b[1].closest_nodes_in_biguint(node_id);
                joined.append(&mut right);
                // select 8 of the closest to node_id
                let mut nodes_id_and_distances = Vec::new(); // (node_id, distance)
                for i in joined.iter() {
                    nodes_id_and_distances.push((i, distance(i, &node_id)));
                }
                nodes_id_and_distances.sort_by_key(|(_, distance)| distance.clone());
                nodes_id_and_distances[0..8]
                    .iter()
                    .map(|(node_id, _)| (*node_id).clone())
                    .collect()
            }
            BucketContent::Nodes(n) => return n.clone(),
        }
    }

    pub fn remove(&mut self, node_id: [u8; 20]) {
        let node_id_big_uint = BigUint::from_bytes_be(&node_id);
        match &mut self.content {
            BucketContent::Buckets(b) => {
                if node_id_big_uint <= b[0].to {
                    b[0].remove(node_id);
                } else {
                    b[1].remove(node_id);
                }
            }
            BucketContent::Nodes(n) => {
                if let Ok(idx) = n.binary_search(&node_id_big_uint) {
                    n.remove(idx);
                }
            }
        }
    }

    pub fn add(&mut self, node_id: [u8; 20]) {
        let node_id_big_uint = BigUint::from_bytes_be(&node_id);
        if node_id_big_uint == self.own_node_id {
            return;
        }
        match &mut self.content {
            BucketContent::Buckets(b) => {
                if node_id_big_uint <= b[0].to {
                    b[0].add(node_id);
                } else {
                    b[1].add(node_id);
                }
            }
            BucketContent::Nodes(n) => {
                if n.contains(&node_id_big_uint) {
                    return;
                }
                if n.len() < MAX_NODES_PER_BUCKET {
                    // insert ordered
                    n.push(node_id_big_uint);
                    n.sort();
                } else if (
                    // if our node id falls withing this bucket
                    self.from <= self.own_node_id && self.own_node_id <= self.to
                ) && (
                    // and there is space to split the bucket
                    self.to.clone() - self.from.clone() > BigUint::from_str("16").unwrap()
                ) {
                    // split this bucket in 2, insert new node
                    let (left_bucket_content, right_bucket_content) = n.split_at(n.len() / 2);
                    let mut left_bucket_content = left_bucket_content.to_vec();
                    let mut right_bucket_content = right_bucket_content.to_vec();
                    let ((a, b), (c, d)) = split(&self.from, &self.to);
                    if node_id_big_uint <= b {
                        left_bucket_content.push(node_id_big_uint);
                        left_bucket_content.sort();
                    } else {
                        right_bucket_content.push(node_id_big_uint);
                        right_bucket_content.sort();
                    }
                    let left_bucket = Bucket {
                        from: a,
                        to: b,
                        content: BucketContent::Nodes(left_bucket_content),
                        own_node_id: self.own_node_id.clone(),
                    };
                    let right_bucket = Bucket {
                        from: c,
                        to: d,
                        content: BucketContent::Nodes(right_bucket_content),
                        own_node_id: self.own_node_id.clone(),
                    };
                    self.content = BucketContent::Buckets(vec![left_bucket, right_bucket])
                }
            }
        }
    }
}

fn split(i: &BigUint, j: &BigUint) -> ((BigUint, BigUint), (BigUint, BigUint)) {
    let mid: BigUint = (j - i) >> 1;
    return (
        (i.clone(), i + mid.clone()),
        (i + mid + BigUint::from_str("1").unwrap(), j.clone()),
    );
}

fn distance(i: &BigUint, j: &BigUint) -> BigUint {
    i ^ j
}

#[cfg(test)]
mod tests {
    use crate::dht::routing_table::{split, BucketContent};
    use num_bigint::BigUint;
    use std::str::FromStr;

    use super::Bucket;

    #[test]
    fn test_split_1() {
        let i = BigUint::from_str("0").unwrap();
        let j = BigUint::from_str("10").unwrap();

        assert_eq!(
            split(&i, &j),
            (
                (
                    BigUint::from_str("0").unwrap(),
                    BigUint::from_str("5").unwrap()
                ),
                (
                    BigUint::from_str("6").unwrap(),
                    BigUint::from_str("10").unwrap()
                )
            )
        );
    }

    #[test]
    fn test_split_2() {
        let i = BigUint::from_str("0").unwrap();
        let j = BigUint::from_str("63").unwrap();

        assert_eq!(
            split(&i, &j),
            (
                (
                    BigUint::from_str("0").unwrap(),
                    BigUint::from_str("31").unwrap()
                ),
                (
                    BigUint::from_str("32").unwrap(),
                    BigUint::from_str("63").unwrap()
                )
            )
        );
    }

    #[test]
    fn test_split_3() {
        let i = BigUint::from_str("31").unwrap();
        let j = BigUint::from_str("63").unwrap();

        assert_eq!(
            split(&i, &j),
            (
                (
                    BigUint::from_str("31").unwrap(),
                    BigUint::from_str("47").unwrap()
                ),
                (
                    BigUint::from_str("48").unwrap(),
                    BigUint::from_str("63").unwrap()
                )
            )
        );
    }

    #[test]
    fn test_add_remove() {
        let max_node_id =
            BigUint::from_str("2").unwrap().pow(160) - BigUint::from_str("1").unwrap();

        let mut b = Bucket::new([0; 20]);
        let mut new_n_1 = [0; 20];
        new_n_1[10] = 1;
        b.add(new_n_1);
        assert_eq!(b, {
            Bucket {
                from: BigUint::from_str("0").unwrap(),
                to: BigUint::from_str("2").unwrap().pow(160) - BigUint::from_str("1").unwrap(),
                content: BucketContent::Nodes(vec![BigUint::from_bytes_be(&new_n_1)]),
                own_node_id: BigUint::from_bytes_be(&[0; 20]),
            }
        });

        let mut new_n_2 = [0; 20];
        new_n_2[0] = 1;
        b.add(new_n_2);
        assert_eq!(b, {
            Bucket {
                from: BigUint::from_str("0").unwrap(),
                to: BigUint::from_str("2").unwrap().pow(160) - BigUint::from_str("1").unwrap(),
                content: BucketContent::Nodes(vec![
                    BigUint::from_bytes_be(&new_n_1),
                    BigUint::from_bytes_be(&new_n_2),
                ]),
                own_node_id: BigUint::from_bytes_be(&[0; 20]),
            }
        });

        let mut new_n_3 = [0; 20];
        new_n_3[19] = 1;
        b.add(new_n_3);
        assert_eq!(b, {
            Bucket {
                from: BigUint::from_str("0").unwrap(),
                to: BigUint::from_str("2").unwrap().pow(160) - BigUint::from_str("1").unwrap(),
                content: BucketContent::Nodes(vec![
                    BigUint::from_bytes_be(&new_n_3),
                    BigUint::from_bytes_be(&new_n_1),
                    BigUint::from_bytes_be(&new_n_2),
                ]),
                own_node_id: BigUint::from_bytes_be(&[0; 20]),
            }
        });

        for i in 0..5 {
            let mut new_n = [0; 20];
            new_n[19] = 10 + i;
            b.add(new_n);
            assert_matches!(b.content, BucketContent::Nodes(ref n_vec) => {
                assert_eq!(n_vec.len(), 4 + i as usize)
            });
        }

        let mut new_n_4 = [0; 20];
        new_n_4[18] = 1;
        b.add(new_n_4);
        assert_matches!(b.content, BucketContent::Buckets(ref b_vec) => {
            assert_matches!(b_vec[0].content, BucketContent::Nodes(ref c_vec) => {
                assert_eq!(c_vec.len(), 5)
            });
            assert_matches!(b_vec[1].content, BucketContent::Nodes(ref c_vec) => {
                assert_eq!(c_vec.len(), 4)
            });
        });

        b.remove(new_n_4);
        assert_matches!(b.content, BucketContent::Buckets(ref b_vec) => {
            assert_matches!(b_vec[0].content, BucketContent::Nodes(ref c_vec) => {
                assert_eq!(c_vec.len(), 4)
            });
            assert_matches!(b_vec[1].content, BucketContent::Nodes(ref c_vec) => {
                assert_eq!(c_vec.len(), 4)
            });
        });
    }

    #[test]
    fn test_closest_nodes() {
        let mut b = Bucket::new([0; 20]);
        for i in 0..255 {
            let mut new_n = [0; 20];
            new_n[10] = i as u8;
            b.add(new_n);
        }
        let mut target = [0; 20];
        target[0] = 0b00000001;
        let mut closest_nodes = b.closest_nodes(&target);
        closest_nodes.sort();

        let b_as_vec = b.as_vec();

        let mut n1 = [0; 20];
        n1[10] = 0b00000001;
        assert!(b_as_vec.contains(&n1));
        let mut n2 = [0; 20];
        n2[10] = 0b00000010;
        assert!(b_as_vec.contains(&n2));
        let mut n3 = [0; 20];
        n3[10] = 0b00000011;
        assert!(b_as_vec.contains(&n3));
        let mut n4 = [0; 20];
        n4[10] = 0b00000100;
        assert!(b_as_vec.contains(&n4));
        let mut n5 = [0; 20];
        n5[10] = 0b00000101;
        assert!(b_as_vec.contains(&n5));
        let mut n6 = [0; 20];
        n6[10] = 0b00000110;
        assert!(b_as_vec.contains(&n6));
        let mut n7 = [0; 20];
        n7[10] = 0b00000111;
        assert!(b_as_vec.contains(&n7));
        let mut n8 = [0; 20];
        n8[10] = 0b00001000;
        assert!(b_as_vec.contains(&n8));
        let mut expected_closest_nodes = vec![n1, n2, n3, n4, n5, n6, n7, n8];
        expected_closest_nodes.sort();
        assert_eq!(closest_nodes, expected_closest_nodes)
    }

    #[test]
    fn test_get() {
        let mut b = Bucket::new([0; 20]);
        for i in 0..255 {
            let mut new_n = [0; 20];
            new_n[10] = i as u8;
            b.add(new_n);
        }
        let mut target = [0; 20];
        target[0] = 0b00000001;
        let ret = b.get(&target);
        assert_matches!(ret, None);

        let mut target = [0; 20];
        target[10] = 0b0000100;
        let ret = b.get(&target);
        assert_matches!(ret, Some(node_id) => {
            assert_eq!(node_id, target)
        });
    }
}
