use std::{net::Ipv4Addr, str::FromStr, time::SystemTime};

use derivative::Derivative;
use num_bigint::BigUint;

pub const K_FACTOR: usize = 8; // maybe we can extend this to 20, like other clients

#[derive(PartialEq, Debug, Clone)]
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
    pub last_replied: SystemTime,
    #[derivative(PartialOrd = "ignore", Ord = "ignore", PartialEq = "ignore")]
    pub last_pinged: SystemTime,
}

impl Node {
    pub fn new(node_id: [u8; 20], addr: Ipv4Addr, port: u16) -> Self {
        Node {
            id: BigUint::from_bytes_be(&node_id),
            addr,
            port,
            last_replied: SystemTime::now(),
            last_pinged: SystemTime::UNIX_EPOCH,
        }
    }

    fn new_fake(node_id: [u8; 20]) -> Self {
        Node {
            id: BigUint::from_bytes_be(&node_id),
            addr: Ipv4Addr::new(127, 0, 0, 1),
            port: 8000,
            last_replied: SystemTime::UNIX_EPOCH,
            last_pinged: SystemTime::UNIX_EPOCH,
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum BucketContent {
    Buckets(Vec<Bucket>), // _always_ 2
    Nodes(Vec<Node>),     // max K_FACTOR
}

pub fn biguint_to_u8_20(n: &BigUint) -> [u8; 20] {
    let mut vec = n.to_bytes_be();
    while vec.len() < 20 {
        vec.insert(0, 0);
    }

    return vec.try_into().expect(
        format!("could not convert biguint to [u8; 20] number is more than 20 bytes: {n}").as_str(),
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

    pub fn as_mut_vec(&mut self) -> Vec<&mut Node> {
        match &mut self.content {
            BucketContent::Buckets(b) => {
                // use split to circumvent double mutable borrowing
                let (b1, b2) = b.split_at_mut(1);
                let mut joined = b1[0].as_mut_vec();
                let mut right = b2[0].as_mut_vec();
                joined.append(&mut right);
                joined
            }
            BucketContent::Nodes(nodes) => {
                let mut ret = Vec::new();
                for n in nodes.iter_mut() {
                    ret.push(n);
                }
                return ret;
            }
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
                nodes_id_and_distances.truncate(K_FACTOR);
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

    // add a node, if present, refresh last_seen, return true if the node was added anew to the routing table
    pub fn add(&mut self, node: Node) -> bool {
        if node.id == self.own_node_id {
            return false;
        }
        match &mut self.content {
            BucketContent::Buckets(b) => {
                if node.id <= b[0].to {
                    return b[0].add(node);
                } else {
                    return b[1].add(node);
                }
            }
            BucketContent::Nodes(bucket_nodes) => {
                if let Ok(i) = bucket_nodes.binary_search(&node) {
                    // the node is already present, refresh last changed
                    bucket_nodes[i].last_replied = SystemTime::now();
                    return false;
                }
                if bucket_nodes.len() < K_FACTOR {
                    // there is space in the bucket, insert ordered
                    bucket_nodes.push(node);
                    bucket_nodes.sort();
                    return true;
                } else if
                // if our node id falls withing this bucket
                self.from <= self.own_node_id && self.own_node_id <= self.to {
                    // split the bucket in 2
                    let ((a, b), (c, d)) = split(&self.from, &self.to);
                    let mut left_content = Vec::new();
                    let mut right_content = Vec::new();

                    // add the previously existing nodes to the new buckets
                    for n in bucket_nodes {
                        if n.id <= b {
                            left_content.push(n.clone());
                        } else {
                            right_content.push(n.clone());
                        }
                    }
                    left_content.sort();
                    right_content.sort();

                    // create the 2 new buckets
                    let mut left_bucket = Bucket {
                        from: a,
                        to: b,
                        content: BucketContent::Nodes(left_content),
                        own_node_id: self.own_node_id.clone(),
                    };
                    let mut right_bucket = Bucket {
                        from: c,
                        to: d,
                        content: BucketContent::Nodes(right_content),
                        own_node_id: self.own_node_id.clone(),
                    };

                    // add the new node
                    let added = if node.id <= left_bucket.to {
                        left_bucket.add(node)
                    } else {
                        right_bucket.add(node)
                    };

                    // update content of this bucket
                    self.content = BucketContent::Buckets(vec![left_bucket, right_bucket]);
                    return added;
                } else {
                    return false;
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
            last_replied: SystemTime::UNIX_EPOCH,
            last_pinged: SystemTime::now(),
        };

        let n2 = Node {
            id: BigUint::from_str("1").unwrap(),
            addr: Ipv4Addr::new(162, 168, 0, 1),
            port: 8081,
            last_replied: SystemTime::now(),
            last_pinged: SystemTime::UNIX_EPOCH,
        };

        let n3 = Node {
            id: BigUint::from_str("2").unwrap(),
            addr: Ipv4Addr::new(162, 168, 0, 1),
            port: 80,
            last_replied: SystemTime::now(),
            last_pinged: SystemTime::UNIX_EPOCH,
        };

        let n4 = Node {
            id: BigUint::from_str("3").unwrap(),
            addr: Ipv4Addr::new(162, 168, 0, 254),
            port: 8082,
            last_replied: SystemTime::now(),
            last_pinged: SystemTime::UNIX_EPOCH,
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
        new_n_1[19] = 10;
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
        new_n_2[19] = 20;
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
        new_n_3[19] = 5;
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
            new_n[18] = 10 + i;
            b.add(Node::new_fake(new_n));
            assert_matches!(b.content, BucketContent::Nodes(ref n_vec) => {
                assert_eq!(n_vec.len(), 4 + i as usize)
            });
        }

        let mut new_n_4 = [0; 20];
        new_n_4[0] = 255; // so that it falls in the other first bucket
        b.add(Node::new_fake(new_n_4));
        assert_matches!(b.content, BucketContent::Buckets(ref b_vec) => {
            assert_matches!(b_vec[0].content, BucketContent::Nodes(ref c_vec) => {
                assert_eq!(c_vec.len(), 8)
            });
            assert_matches!(b_vec[1].content, BucketContent::Nodes(ref c_vec) => {
                assert_eq!(c_vec.len(), 1)
            });
        });

        b.remove(&Node::new_fake(new_n_4));
        assert_matches!(b.content, BucketContent::Buckets(ref b_vec) => {
            assert_matches!(b_vec[0].content, BucketContent::Nodes(ref c_vec) => {
                assert_eq!(c_vec.len(), 8)
            });
            assert_matches!(b_vec[1].content, BucketContent::Nodes(ref c_vec) => {
                assert_eq!(c_vec.len(), 0)
            });
        });

        b.remove(&Node::new_fake(new_n_3));
        assert_matches!(b.content, BucketContent::Buckets(ref b_vec) => {
            assert_matches!(b_vec[0].content, BucketContent::Nodes(ref c_vec) => {
                assert_eq!(c_vec.len(), 7)
            });
            assert_matches!(b_vec[1].content, BucketContent::Nodes(ref c_vec) => {
                assert_eq!(c_vec.len(), 0)
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

        let mut n1h = [0; 20];
        n1h[10] = 0b00000001;
        let n1 = Node::new_fake(n1h);
        assert_matches!(b.get_mut(&n1h), Some(_));
        let mut n2h = [0; 20];
        n2h[10] = 0b00000010;
        let n2 = Node::new_fake(n2h);
        assert_matches!(b.get_mut(&n2h), Some(_));
        let mut n3h = [0; 20];
        n3h[10] = 0b00000011;
        let n3 = Node::new_fake(n3h);
        assert_matches!(b.get_mut(&n3h), Some(_));
        let mut n4h = [0; 20];
        n4h[10] = 0b00000100;
        let n4 = Node::new_fake(n4h);
        assert_matches!(b.get_mut(&n4h), Some(_));
        let mut n5h = [0; 20];
        n5h[10] = 0b00000101;
        let n5 = Node::new_fake(n5h);
        assert_matches!(b.get_mut(&n5h), Some(_));
        let mut n6h = [0; 20];
        n6h[10] = 0b00000110;
        let n6 = Node::new_fake(n6h);
        assert_matches!(b.get_mut(&n6h), Some(_));
        let mut n7h = [0; 20];
        n7h[10] = 0b00000111;
        let n7 = Node::new_fake(n7h);
        assert_matches!(b.get_mut(&n7h), Some(_));
        let mut n8h = [0; 20];
        n8h[10] = 0b00001000;
        let n8 = Node::new_fake(n8h);
        assert_matches!(b.get_mut(&n8h), Some(_));
        let mut expected_closest_nodes = vec![n1, n2, n3, n4, n5, n6, n7, n8];
        expected_closest_nodes.sort();
        assert_eq!(closest_nodes, expected_closest_nodes)
    }

    #[test]
    fn test_get_mut() {
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
