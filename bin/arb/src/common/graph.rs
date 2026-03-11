use std::collections::HashMap;

const INF: i64 = i64::MAX / 4;

#[derive(Clone, Debug)]
pub struct DirectedEdge {
    pub from: usize,
    pub to: usize,
    pub weight: i64,
}

#[derive(Clone, Debug, Default)]
pub struct WeightedDigraph {
    nodes: Vec<String>,
    node_to_idx: HashMap<String, usize>,
    edges: Vec<DirectedEdge>,
}

#[derive(Clone, Debug)]
pub struct ShortestPathResult {
    nodes: Vec<String>,
    node_to_idx: HashMap<String, usize>,
    dist: Vec<i64>,
    prev: Vec<Option<usize>>,
}

impl WeightedDigraph {
    pub fn add_node(&mut self, node: impl Into<String>) -> usize {
        let node = node.into();
        if let Some(idx) = self.node_to_idx.get(&node) {
            return *idx;
        }

        let idx = self.nodes.len();
        self.nodes.push(node.clone());
        self.node_to_idx.insert(node, idx);
        idx
    }

    pub fn add_edge(&mut self, from: impl Into<String>, to: impl Into<String>, weight: i64) {
        let from_idx = self.add_node(from);
        let to_idx = self.add_node(to);
        self.edges.push(DirectedEdge {
            from: from_idx,
            to: to_idx,
            weight,
        });
    }

    pub fn bellman_ford(&self, source: &str) -> Option<ShortestPathResult> {
        let n = self.nodes.len();
        if n == 0 {
            return None;
        }
        let source_idx = *self.node_to_idx.get(source)?;

        let mut dist = vec![INF; n];
        let mut prev = vec![None; n];
        dist[source_idx] = 0;

        for _ in 0..n.saturating_sub(1) {
            let mut updated = false;
            for e in &self.edges {
                if dist[e.from] == INF {
                    continue;
                }
                let cand = dist[e.from].saturating_add(e.weight);
                if cand < dist[e.to] {
                    dist[e.to] = cand;
                    prev[e.to] = Some(e.from);
                    updated = true;
                }
            }
            if !updated {
                break;
            }
        }

        // Keep behavior deterministic and safe: if a negative cycle exists, caller should fallback.
        for e in &self.edges {
            if dist[e.from] == INF {
                continue;
            }
            if dist[e.from].saturating_add(e.weight) < dist[e.to] {
                return None;
            }
        }

        Some(ShortestPathResult {
            nodes: self.nodes.clone(),
            node_to_idx: self.node_to_idx.clone(),
            dist,
            prev,
        })
    }
}

impl ShortestPathResult {
    pub fn distance_to(&self, target: &str) -> Option<i64> {
        let idx = *self.node_to_idx.get(target)?;
        let d = self.dist[idx];
        if d == INF {
            None
        } else {
            Some(d)
        }
    }

    pub fn path_to(&self, target: &str) -> Option<Vec<String>> {
        let mut idx = *self.node_to_idx.get(target)?;
        if self.dist[idx] == INF {
            return None;
        }

        let mut path = vec![self.nodes[idx].clone()];
        let mut hops = 0usize;
        while let Some(prev_idx) = self.prev[idx] {
            path.push(self.nodes[prev_idx].clone());
            idx = prev_idx;
            hops += 1;
            if hops > self.nodes.len() {
                return None;
            }
        }

        path.reverse();
        Some(path)
    }
}

#[cfg(test)]
mod tests {
    use super::WeightedDigraph;

    #[test]
    fn bellman_ford_find_shortest_path() {
        let mut g = WeightedDigraph::default();
        g.add_edge("A", "B", 1);
        g.add_edge("B", "C", 1);
        g.add_edge("A", "C", 3);

        let result = g.bellman_ford("A").expect("path should exist");
        assert_eq!(result.distance_to("C"), Some(2));
        assert_eq!(
            result.path_to("C").expect("path to C"),
            vec!["A".to_string(), "B".to_string(), "C".to_string()]
        );
    }
}
