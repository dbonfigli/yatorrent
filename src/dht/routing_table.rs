use std::{net::Ipv4Addr, str::FromStr, time::SystemTime};

use derivative::Derivative;
use num_bigint::BigUint;

#[derive(PartialEq, Debug)]
pub struct Bucket {
    pub from: BigUint,
    pub to: BigUint,
    pub content: BucketContent,
    own_node_id: BigUint,
}

#[derive(Derivative, Debug, Clone)]
#[derivative(PartialOrd, Ord, PartialEq, Eq)]
pub struct Node {
    pub id: BigUint,

    #[derivative(PartialOrd = "ignore", Ord = "ignore", PartialEq = "ignore")]
    pub addr: Ipv4Addr,
    #[derivative(PartialOrd = "ignore", Ord = "ignore", PartialEq = "ignore")]
    pub port: u16,
    #[derivative(PartialOrd = "ignore", Ord = "ignore", PartialEq = "ignore")]
    pub last_changed: SystemTime,
}

impl Node {
    pub fn new(node_id: [u8; 20], addr: Ipv4Addr, port: u16) -> Self {
        Node {
            id: BigUint::from_bytes_be(&node_id),
            addr,
            port,
            last_changed: SystemTime::now(),
        }
    }

    fn new_fake(node_id: [u8; 20]) -> Self {
        Node {
            id: BigUint::from_bytes_be(&node_id),
            addr: Ipv4Addr::new(127, 0, 0, 1),
            port: 8000,
            last_changed: SystemTime::UNIX_EPOCH,
        }
    }
}

#[derive(PartialEq, Debug)]
pub enum BucketContent {
    Buckets(Vec<Bucket>), // always 2
    Nodes(Vec<Node>),     // max MAX_NODES_PER_BUCKET
}

static MAX_NODES_PER_BUCKET: usize = 8;

pub fn biguint_to_u8_20(n: &BigUint) -> [u8; 20] {
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
    pub fn new(own_node_id: &[u8; 20]) -> Self {
        return Bucket {
            from: BigUint::from_str("0").unwrap(),
            to: BigUint::from_str("2").unwrap().pow(160) - BigUint::from_str("1").unwrap(),
            content: BucketContent::Nodes(Vec::new()),
            own_node_id: BigUint::from_bytes_be(own_node_id),
        };
    }

    pub fn get_mut(&mut self, node_id: &[u8; 20]) -> Option<&mut Node> {
        let fake_requesting_node = Node::new_fake(node_id.clone());
        match &mut self.content {
            BucketContent::Buckets(b) => {
                if fake_requesting_node.id <= b[0].to {
                    b[0].get_mut(node_id)
                } else {
                    b[1].get_mut(node_id)
                }
            }
            BucketContent::Nodes(n) => {
                if let Ok(i) = n.binary_search(&fake_requesting_node) {
                    Some(&mut n[i])
                } else {
                    None
                }
            }
        }
    }

    pub fn as_vec(&self) -> Vec<Node> {
        match &self.content {
            BucketContent::Buckets(b) => {
                let mut joined = b[0].as_vec();
                let mut right = b[1].as_vec();
                joined.append(&mut right);
                joined
            }
            BucketContent::Nodes(n) => n.clone(),
        }
    }

    pub fn closest_nodes(&self, node_id: &[u8; 20]) -> Vec<Node> {
        let fake_requesting_node = Node::new_fake(node_id.clone());
        self.closest_nodes_by_node(&fake_requesting_node)
    }

    // todo: optimize this
    fn closest_nodes_by_node(&self, node: &Node) -> Vec<Node> {
        match &self.content {
            BucketContent::Buckets(b) => {
                let mut joined = b[0].closest_nodes_by_node(node);
                let mut right = b[1].closest_nodes_by_node(node);
                joined.append(&mut right);
                // select N of the closest to node_id
                let mut nodes_id_and_distances = Vec::new(); // (node_id, distance)
                for i in joined.iter() {
                    nodes_id_and_distances.push((i, distance_biguint(&i.id, &node.id)));
                }
                nodes_id_and_distances.sort_by_key(|(_, distance)| distance.clone());
                nodes_id_and_distances.truncate(MAX_NODES_PER_BUCKET);
                nodes_id_and_distances
                    .iter()
                    .map(|(node_id, _)| (*node_id).clone())
                    .collect()
            }
            BucketContent::Nodes(n) => return n.clone(),
        }
    }

    pub fn remove(&mut self, node: &Node) {
        match &mut self.content {
            BucketContent::Buckets(b) => {
                if node.id <= b[0].to {
                    b[0].remove(node);
                } else {
                    b[1].remove(node);
                }
            }
            BucketContent::Nodes(n) => {
                if let Ok(idx) = n.binary_search(&node) {
                    n.remove(idx);
                }
            }
        }
    }

    // add a node, if present, refresh last_seen
    pub fn add(&mut self, node: Node) {
        if node.id == self.own_node_id {
            return;
        }
        match &mut self.content {
            BucketContent::Buckets(b) => {
                if node.id <= b[0].to {
                    b[0].add(node);
                } else {
                    b[1].add(node);
                }
            }
            BucketContent::Nodes(n) => {
                if let Ok(i) = n.binary_search(&node) {
                    n[i].last_changed = SystemTime::now();
                    return;
                }
                if n.len() < MAX_NODES_PER_BUCKET {
                    // insert ordered
                    n.push(node);
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
                    if node.id <= b {
                        left_bucket_content.push(node);
                        left_bucket_content.sort();
                    } else {
                        right_bucket_content.push(node);
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

fn distance_biguint(i: &BigUint, j: &BigUint) -> BigUint {
    i ^ j
}

pub fn distance(i: &[u8; 20], j: &[u8; 20]) -> BigUint {
    distance_biguint(&BigUint::from_bytes_be(i), &BigUint::from_bytes_be(j))
}

#[cfg(test)]
mod tests {
    use crate::dht::routing_table::{split, BucketContent};
    use num_bigint::BigUint;
    use std::{net::Ipv4Addr, str::FromStr, time::SystemTime};

    use super::{Bucket, Node};

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
    fn test_comparison_with_derivative() {
        let n1 = Node {
            id: BigUint::from_str("1").unwrap(),
            addr: Ipv4Addr::new(127, 0, 0, 1),
            port: 8080,
            last_changed: SystemTime::UNIX_EPOCH,
        };

        let n2 = Node {
            id: BigUint::from_str("1").unwrap(),
            addr: Ipv4Addr::new(162, 168, 0, 1),
            port: 8081,
            last_changed: SystemTime::now(),
        };

        let n3 = Node {
            id: BigUint::from_str("2").unwrap(),
            addr: Ipv4Addr::new(162, 168, 0, 1),
            port: 80,
            last_changed: SystemTime::now(),
        };

        let n4 = Node {
            id: BigUint::from_str("3").unwrap(),
            addr: Ipv4Addr::new(162, 168, 0, 254),
            port: 8082,
            last_changed: SystemTime::now(),
        };

        assert!(n1 == n2);
        assert!(n1 <= n2);
        assert!(n2 < n3);
        assert!(n1 < n3);
        assert!(n3 < n4);
    }

    #[test]
    fn test_add_remove() {
        let mut b = Bucket::new(&[0; 20]);
        let mut new_n_1 = [0; 20];
        new_n_1[10] = 1;
        b.add(Node::new_fake(new_n_1));
        assert_eq!(b, {
            Bucket {
                from: BigUint::from_str("0").unwrap(),
                to: BigUint::from_str("2").unwrap().pow(160) - BigUint::from_str("1").unwrap(),
                content: BucketContent::Nodes(vec![Node::new_fake(new_n_1)]),
                own_node_id: BigUint::from_bytes_be(&[0; 20]),
            }
        });

        let mut new_n_2 = [0; 20];
        new_n_2[0] = 1;
        b.add(Node::new_fake(new_n_2));
        assert_eq!(b, {
            Bucket {
                from: BigUint::from_str("0").unwrap(),
                to: BigUint::from_str("2").unwrap().pow(160) - BigUint::from_str("1").unwrap(),
                content: BucketContent::Nodes(vec![
                    Node::new_fake(new_n_1),
                    Node::new_fake(new_n_2),
                ]),
                own_node_id: BigUint::from_bytes_be(&[0; 20]),
            }
        });

        let mut new_n_3 = [0; 20];
        new_n_3[19] = 1;
        b.add(Node::new_fake(new_n_3));
        assert_eq!(b, {
            Bucket {
                from: BigUint::from_str("0").unwrap(),
                to: BigUint::from_str("2").unwrap().pow(160) - BigUint::from_str("1").unwrap(),
                content: BucketContent::Nodes(vec![
                    Node::new_fake(new_n_3),
                    Node::new_fake(new_n_1),
                    Node::new_fake(new_n_2),
                ]),
                own_node_id: BigUint::from_bytes_be(&[0; 20]),
            }
        });

        for i in 0..5 {
            let mut new_n = [0; 20];
            new_n[19] = 10 + i;
            b.add(Node::new_fake(new_n));
            assert_matches!(b.content, BucketContent::Nodes(ref n_vec) => {
                assert_eq!(n_vec.len(), 4 + i as usize)
            });
        }

        let mut new_n_4 = [0; 20];
        new_n_4[18] = 1;
        b.add(Node::new_fake(new_n_4));
        assert_matches!(b.content, BucketContent::Buckets(ref b_vec) => {
            assert_matches!(b_vec[0].content, BucketContent::Nodes(ref c_vec) => {
                assert_eq!(c_vec.len(), 5)
            });
            assert_matches!(b_vec[1].content, BucketContent::Nodes(ref c_vec) => {
                assert_eq!(c_vec.len(), 4)
            });
        });

        b.remove(&Node::new_fake(new_n_4));
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
        let mut b = Bucket::new(&[0; 20]);
        for i in 0..255 {
            let mut new_n = [0; 20];
            new_n[10] = i as u8;
            b.add(Node::new_fake(new_n));
        }
        let mut target = [0; 20];
        target[0] = 0b00000001;
        let mut closest_nodes = b.closest_nodes(&target);
        closest_nodes.sort();

        let b_as_vec = b.as_vec();

        let mut n1 = [0; 20];
        n1[10] = 0b00000001;
        let n1 = Node::new_fake(n1);
        assert!(b_as_vec.contains(&n1));
        let mut n2 = [0; 20];
        n2[10] = 0b00000010;
        let n2 = Node::new_fake(n2);
        assert!(b_as_vec.contains(&n2));
        let mut n3 = [0; 20];
        n3[10] = 0b00000011;
        let n3 = Node::new_fake(n3);
        assert!(b_as_vec.contains(&n3));
        let mut n4 = [0; 20];
        n4[10] = 0b00000100;
        let n4 = Node::new_fake(n4);
        assert!(b_as_vec.contains(&n4));
        let mut n5 = [0; 20];
        n5[10] = 0b00000101;
        let n5 = Node::new_fake(n5);
        assert!(b_as_vec.contains(&n5));
        let mut n6 = [0; 20];
        n6[10] = 0b00000110;
        let n6 = Node::new_fake(n6);
        assert!(b_as_vec.contains(&n6));
        let mut n7 = [0; 20];
        n7[10] = 0b00000111;
        let n7 = Node::new_fake(n7);
        assert!(b_as_vec.contains(&n7));
        let mut n8 = [0; 20];
        n8[10] = 0b00001000;
        let n8 = Node::new_fake(n8);
        assert!(b_as_vec.contains(&n8));
        let mut expected_closest_nodes = vec![n1, n2, n3, n4, n5, n6, n7, n8];
        expected_closest_nodes.sort();
        assert_eq!(closest_nodes, expected_closest_nodes)
    }

    #[test]
    fn test_get() {
        let mut b = Bucket::new(&[0; 20]);
        for i in 0..255 {
            let mut new_n = [0; 20];
            new_n[10] = i as u8;
            b.add(Node::new_fake(new_n));
        }
        let mut target = [0; 20];
        target[0] = 0b00000001;
        let ret = b.get_mut(&target);
        assert_matches!(ret, None);

        let mut target = [0; 20];
        target[10] = 0b0000100;
        let ret = b.get_mut(&target);
        assert_matches!(ret, Some(node) => {
            assert_eq!(*node, Node::new_fake(target))
        });
    }
}
