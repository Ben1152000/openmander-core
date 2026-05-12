use std::collections::{HashSet, VecDeque};

use rand::{distributions::{weighted::WeightedIndex, Distribution}, Rng};

use crate::partition::Partition;

/// Statistics returned by `equalize_exact`.
pub(crate) struct EqualizeStats {
    /// Number of census blocks reassigned to a new district.
    pub blocks_moved: usize,
    /// Number of spanning-tree edges for which the ILP found no feasible
    /// solution and the edge was left at zero flow (boundary unchanged).
    pub fallback_edges: usize,
}

impl Partition {
    /// Find the part with the minimum total weight.
    /// Returns (part, part_weight).
    fn part_with_min_weight(&self, series: &str) -> (u32, f64) {
        assert!(self.num_parts() > 1, "cannot find min part with only one part");
        assert!(self.unit_weights().contains(series),
            "series '{}' not found in node weights", series);

        (1..self.num_parts())
            .map(|p| (p, self.part_weights().get_as_f64(series, p as usize).unwrap()))
            .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
            .unwrap()
    }

    /// Attempt to find neighboring parts to a given part by sampling its frontier.
    /// `samples` is the number of random frontier nodes to sample.
    /// Use this function when computing the full neighbor set is too expensive.
    fn sample_neighboring_parts(&self, part: u32, samples: usize, rng: &mut impl Rng) -> Vec<u32> {
        assert!(part < self.num_parts(), "part {} out of range", part);

        let frontier = self.frontiers.get(part as usize);
        if frontier.is_empty() { return vec![] }

        let mut neighbors = HashSet::new();
        for _ in 0..samples {
            let node = frontier[rng.gen_range(0..frontier.len())];
            neighbors.extend(self.graph().edges(node)
                .map(|u| self.assignment(u))
                .filter(|&p| p != 0 && p != part));
        }

        neighbors.into_iter().collect()
    }

    /// Equalize total weights between two parts using greedy swaps.
    /// `series` should name a column in node_weights.series.
    pub(crate) fn equalize_parts(&mut self, series: &str, a: u32, b: u32, tolerance: f64) {
        // Validate parts and adjacency.
        assert!(a < self.num_parts() && b < self.num_parts() && a != b,
            "a and b must be distinct parts in range [0, {})", self.num_parts());

        let mut rng = rand::thread_rng();

        // Define src as the part with surplus weight.
        let a_total = self.part_weights().get_as_f64(series, a as usize).unwrap();
        let b_total = self.part_weights().get_as_f64(series, b as usize).unwrap();
        let (src, dest, src_total, dest_total) =
            if a_total >= b_total { (a, b, a_total, b_total) }
            else { (b, a, b_total, a_total) };

        let delta = src_total - dest_total;
        let mut remaining = delta / 2.0;

        while remaining > 0.0 {
            // Pick a random candidate on the boundary of src.
            let candidates = self.frontiers.get(src as usize);
            let node = candidates[rng.gen_range(0..candidates.len())];

            // Skip if not adjacent.
            if !(self.part_is_empty(dest) || self.node_borders_part(node, dest)) { continue }

            if self.check_node_contiguity(node, dest) {
                let delta = self.unit_weights().get_as_f64(series, node).unwrap();
                self.move_node(node, dest, false);
                remaining -= delta;
            } else {
                // Compute articulation bundle and move node with it (if necessary).
                let mut subgraph = self.cut_subgraph_within_part(node);
                subgraph.push(node);

                let delta = subgraph.iter()
                    .map(|&u| self.unit_weights().get_as_f64(series, u).unwrap())
                    .sum::<f64>();
                self.move_subgraph(&subgraph, dest, false);
                remaining -= delta;
            }
        }

        // If we overshot, recursively equalize in the other direction with higher tolerance.
        if -remaining > tolerance { self.equalize_parts(series, a, b, tolerance * 1.2) }
    }

    /// Run one outer iteration of equalization. Returns `true` if all parts are within tolerance.
    /// Intended for chunked execution from JS: call in a loop, yielding between calls.
    pub(crate) fn equalize_step(&mut self, series: &str, tolerance: f64) -> bool {
        assert_ne!(self.num_parts(), 1, "cannot equalize with only one part");
        assert!(self.unit_weights().contains(series),
            "series '{}' not found in node weights", series);

        let mut rng = rand::thread_rng();

        let total = (1..self.num_parts())
            .map(|p| self.part_weights().get_as_f64(series, p as usize).unwrap())
            .sum::<f64>();
        let target = total / ((self.num_parts() - 1) as f64);
        let allowed = target * tolerance;

        let totals = (1..self.num_parts())
            .map(|p| self.part_weights().get_as_f64(series, p as usize).unwrap())
            .collect::<Vec<_>>();
        let deviations = totals.iter()
            .map(|&t| (t - target).abs())
            .collect::<Vec<_>>();
        let largest_deviation = *deviations.iter()
            .max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();

        if largest_deviation <= allowed {
            println!("Target population per part: {:.0} ±{:.0}", target, allowed);
            println!("Equalization complete, max deviation {:.0}", largest_deviation);
            return true;
        }

        let distribution = WeightedIndex::new(&deviations).unwrap();
        let part = distribution.sample(&mut rng) as u32 + 1;

        if totals[part as usize - 1] > target * 2.0 {
            let (smallest, _) = self.part_with_min_weight(series);
            let neighbors = self.sample_neighboring_parts(smallest, 8, &mut rng);
            if let Some((neighbor, _)) = neighbors.iter()
                .map(|&p| (p, self.part_weights().get_as_f64(series, p as usize).unwrap()))
                .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
                && let Some(new_part) = self.merge_parts(neighbor, smallest, false) {
                    let frontier = self.frontiers.get(part as usize);
                    if !frontier.is_empty() {
                        let node = frontier[rng.gen_range(0..frontier.len())];
                        self.move_node_with_articulation(node, new_part);
                        self.equalize_parts(series, part, new_part, largest_deviation / 2.0);
                        return false;
                    }
                }
        }

        let neighbors = self.sample_neighboring_parts(part, 8, &mut rng);
        if neighbors.is_empty() { return false; }
        let other = neighbors[rng.gen_range(0..neighbors.len())];
        self.equalize_parts(series, part, other, largest_deviation / 2.0);
        false
    }

    /// Equalize total weights across all parts using greedy swaps.
    /// `series` should name a column in node_weights.series.
    /// `tolerance` is the allowed fraction deviation from ideal (e.g. 0.01 = ±1%).
    /// `max_iter` is the maximum number of equalization passes to attempt.
    pub(crate) fn equalize(&mut self, series: &str, tolerance: f64, max_iter: usize) {
        assert_ne!(self.num_parts(), 1, "cannot equalize with only one part");
        assert!(self.unit_weights().contains(series),
            "series '{}' not found in node weights", series);

        let mut rng = rand::thread_rng();

        // Compute target population and tolerance band (ignoring unassigned part 0).
        let total = (1..self.num_parts())
            .map(|part| self.part_weights().get_as_f64(series, part as usize).unwrap())
            .sum::<f64>();
        let target = total / ((self.num_parts() - 1) as f64);
        let allowed = target * tolerance;

        println!("Target population per part: {:.0} ±{:.0}", target, allowed);

        // Iterate until all parts are within tolerance, or we give up.
        for i in 0..max_iter {
            let totals = (1..self.num_parts())
                .map(|p| self.part_weights().get_as_f64(series, p as usize).unwrap())
                .collect::<Vec<_>>();
            let deviations = totals.iter()
                .map(|&total| (total - target).abs())
                .collect::<Vec<_>>();

            // Find the worst-offending part (max absolute deviation).
            let largest_deviation = *deviations.iter()
                .max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();

            // Exit if all parts are within tolerance.
            if largest_deviation <= allowed {
                println!("Equalization complete after {} iterations, max deviation {:.0}", i, largest_deviation);
                return;
            }

            // Select a random part (weighted by absolute deviation)
            let distribution = WeightedIndex::new(&deviations).unwrap();
            let part = distribution.sample(&mut rng) as u32 + 1;

            // If the part total is more than twice the target, split into two districts while the smallest.
            if totals[part as usize - 1] > target * 2.0 {
                let (smallest, _) = self.part_with_min_weight(series);
                let (neighbor, _) = self.sample_neighboring_parts(smallest, 8, &mut rng).iter()
                    .map(|&p| (p, self.part_weights().get_as_f64(series, p as usize).unwrap()))
                    .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
                    .unwrap();

                // If merged successfully, assign a random frontier to the eliminated district and equalize with part
                if let Some(new_part) = self.merge_parts(neighbor, smallest, false) {
                    let frontier = self.frontiers.get(part as usize);
                    if !frontier.is_empty() {
                        let node = frontier[rng.gen_range(0..frontier.len())];
                        self.move_node_with_articulation(node, new_part);
                        self.equalize_parts(series, part, new_part, largest_deviation / 2.0);
                        continue;
                    }
                }
            }

            // Pick random neighboring part and equalize.
            let neighbors = self.sample_neighboring_parts(part, 8, &mut rng);
            if neighbors.is_empty() { continue }

            // Pick random neighbor
            let neighbors = neighbors.into_iter().collect::<Vec<_>>();
            let other = neighbors[rng.gen_range(0..neighbors.len())];

            self.equalize_parts(series, part, other, largest_deviation / 2.0);
        }

        println!("Equalization incomplete, max deviation {:.0}",
            (1..self.num_parts())
                .map(|p| self.part_weights().get_as_f64(series, p as usize).unwrap())
                .map(|total| (total - target).abs())
                .max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap()
        );
    }






    // -------------------------------------------------------------------------
    // Exact equalization (population deviation 0 or 1)
    // -------------------------------------------------------------------------
    //
    // Algorithm overview (four phases):
    //
    //  Phase 1 — Build the equalization graph + spanning tree
    //    Construct a graph whose nodes are the N real districts and whose edges
    //    connect pairs that share at least one within-county block-level
    //    boundary (swapping across a county boundary would create a new county
    //    split, so cross-county boundaries are excluded).  For each edge, record
    //    the immediate frontier blocks on both sides (the k=0 candidates for
    //    transfer).  Then compute a max-weight spanning tree (Prim's, weighted
    //    by frontier capacity) to route population flow: each tree edge carries
    //    a uniquely-determined net_flow given the global {T, T+1} assignment;
    //    cycle edges carry zero flow by default.
    //
    //  Phase 2 — Precompute per-edge transfer menus
    //    For each tree edge, enumerate the feasible net_flows (one per feasible
    //    k in its subtree_k_range) and solve a small branch-and-bound ILP over
    //    transferable regions for each flow.  For each cycle edge, solve only
    //    net_flow = 0 (no transfer needed by default).  Results are cached in
    //    an EdgeMenu keyed by net_flow so Phase 3 can look them up in O(1).
    //
    //  Phase 3 — Solve the global excess assignment
    //    Branch-and-bound over all ways to assign exactly r of the N districts
    //    to the T+1 target.  For each candidate assignment, the spanning tree
    //    formula determines the net_flow on every tree edge; prune as soon as
    //    any tree edge's flow has no feasible ILP solution in its EdgeMenu.
    //    If the tree proves globally infeasible, reroute flow through an
    //    adjacent cycle edge, solve its ILP on demand, and resume.  Returns
    //    the chosen net_flow per edge (indexed by edge index, 0 for unused
    //    edges) so Phase 4 can assemble the final transfer list.
    //
    //  Phase 4 — Apply transfers
    //    Look up the chosen EdgeMenu solution for each edge with non-zero flow
    //    and execute the block reassignments, updating the partition's
    //    assignment, frontier, and weight caches.
    //
    // -------------------------------------------------------------------------

    /// Main entry point. Given the current partition, attempts to reassign
    /// census blocks so that every district reaches its target population
    /// (either T or T+1). Returns statistics about the equalization run.
    ///
    /// `county_ids[u]` is the county index for block u (e.g. derived from the
    /// first 5 characters of the block's geo_id, mapped to a contiguous index
    /// by the caller).  Two adjacent blocks with the same county_id share a
    /// within-county boundary eligible for block exchange.
    pub(crate) fn equalize_exact(&mut self, series: &str, county_ids: &[u32]) -> EqualizeStats {
        let graph = self.build_equalization_graph(series, county_ids);
        let tree  = graph.compute_spanning_tree();
        let num_tree_edges = tree.tree_edges.len();

        // Phase 2 — precompute transfer menus for all tree edges.
        let mut menus: std::collections::HashMap<usize, EdgeMenu> =
            std::collections::HashMap::new();
        for &ei in &tree.tree_edges.clone() {
            let menu = self.precompute_edge_menu(ei, &graph, &tree, series);
            menus.insert(ei, menu);
        }

        // Phase 3 — find a globally consistent excess assignment.
        let (flows, fallback_edge_count) =
            match Self::solve_excess_assignment(&graph, &tree, &mut menus, series) {
                Some((f, fb)) => (f, fb),
                None          => return EqualizeStats { blocks_moved: 0, fallback_edges: num_tree_edges },
            };

        // Collect all transfers for non-zero edges.
        let mut all_transfers: Vec<Transfer> = Vec::new();
        for &ei in &tree.tree_edges {
            let flow = flows[ei];
            if flow == 0 { continue; }
            if let Some(Some(transfers)) = menus.get(&ei).and_then(|m| m.solutions.get(&flow)) {
                all_transfers.extend(transfers.iter().map(|t| Transfer {
                    from: t.from,
                    to:   t.to,
                    nodes: t.nodes.clone(),
                }));
            }
        }

        // Phase 4 — apply all transfers (joint contiguity filter included).
        let blocks_moved = self.apply_exact_transfers(&all_transfers);
        EqualizeStats { blocks_moved, fallback_edges: fallback_edge_count }
    }

    // -------------------------------------------------------------------------
    // Debug
    // -------------------------------------------------------------------------

    /// Build the equalization graph and spanning tree for the current partition,
    /// print a structured summary to stdout, and return a one-line summary string.
    ///
    /// Intended for development: call from the frontend to see the graph
    /// structure and feasible net_flows for the current map state.
    pub(crate) fn debug_equalization_graph(&mut self, series: &str, county_ids: &[u32]) -> String {
        let graph = self.build_equalization_graph(series, county_ids);
        let tree  = graph.compute_spanning_tree();

        let n         = graph.pops.len();
        let total_pop: i64 = graph.pops.iter().sum();
        let n_tree    = tree.tree_edges.len();
        let n_cycle   = graph.edges.len() - n_tree;
        let t         = graph.base_target as i64;

        println!("EqGraph  N={n}  T={t}  r={}  total={total_pop}", graph.num_extra);
        println!("{} edges  ({n_tree} tree, {n_cycle} cycle)", graph.edges.len());
        println!();

        // ---- Tree edge table ------------------------------------------------
        println!("{:<4}  {:<12}  {:<14}  {}", "ei", "edge", "direction", "people to move");
        println!("{}", "-".repeat(52));
        for &ei in &tree.tree_edges {
            let e     = &graph.edges[ei];
            let flows = &tree.tree_edge_net_flows[ei];

            // Determine transfer direction and amount in plain terms.
            // flows are in a→b convention; all flows for a given tree edge have the
            // same sign (direction), so we look at the first non-zero value.
            let all_nonneg = flows.iter().all(|&f| f >= 0);
            let (from, to, amounts): (u32, u32, Vec<i64>) = if all_nonneg {
                (e.a, e.b, flows.iter().copied().collect())
            } else {
                (e.b, e.a, flows.iter().map(|&f| -f).collect())
            };

            // Format the amounts as "X" or "X or Y" (deduplicated, sorted).
            let mut amounts = amounts;
            amounts.sort_unstable();
            amounts.dedup();
            let amount_str = if amounts.len() == 1 {
                format!("{}", amounts[0])
            } else {
                amounts.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(" or ")
            };

            println!("{:<4}  d{:<2} <-> d{:<2}   d{} → d{:<4}  {}",
                ei, e.a, e.b, from, to, amount_str);
        }

        // ---- Block transfers per tree edge ----------------------------------
        println!();
        println!("Block transfers per tree edge:");
        println!("{}", "-".repeat(52));
        for ei in tree.tree_edges.clone() {
            let e = &graph.edges[ei];
            println!("ei={ei}  d{} <-> d{}:", e.a, e.b);
            let menu = self.precompute_edge_menu(ei, &graph, &tree, series);
            let mut flows: Vec<i64> = menu.solutions.keys().copied().collect();
            flows.sort_unstable();
            for flow in flows {
                let pop: u64 = flow.unsigned_abs();
                let (from, to) = if flow >= 0 { (e.a, e.b) } else { (e.b, e.a) };
                match &menu.solutions[&flow] {
                    None => println!("  flow={flow:+} ({pop} ppl d{from}→d{to}): no feasible transfer"),
                    Some(transfers) if transfers.is_empty() => {
                        println!("  flow={flow:+}: (none)");
                    }
                    Some(transfers) => {
                        let node_pops: Vec<u64> = transfers.iter()
                            .flat_map(|t| &t.nodes)
                            .map(|&u| self.unit_weights().get_as_f64(series, u)
                                .unwrap_or(0.0).round() as u64)
                            .collect();
                        let nodes: Vec<usize> = transfers.iter()
                            .flat_map(|t| &t.nodes)
                            .copied()
                            .collect();
                        let pop_str = node_pops.iter()
                            .map(|p| p.to_string())
                            .collect::<Vec<_>>()
                            .join("+");
                        println!("  flow={flow:+} ({pop} ppl d{from}→d{to}): {} block(s) [pop={pop_str}]  nodes={nodes:?}",
                            nodes.len());
                    }
                }
            }
        }

        format!("EqGraph: N={n}, T={t}, r={}, {} edges ({n_tree} tree, {n_cycle} cycle)",
            graph.num_extra, graph.edges.len())
    }

    // -------------------------------------------------------------------------
    // Phase 1 — Build the equalization graph
    // -------------------------------------------------------------------------

    /// Build the equalization graph for the current partition.
    ///
    /// An edge (a, b) is included iff districts a and b share at least one
    /// block adjacency that does not lie entirely on a county boundary (i.e.,
    /// swapping blocks across it would not create a new county split).  For
    /// each included edge the function collects the immediate frontier blocks
    /// on both sides (the k=0 candidates for transfer).
    ///
    /// Call `EqualizationGraph::compute_spanning_tree` on the result to route
    /// population flow before proceeding to Phase 2.
    ///
    /// `county_ids[u]` is the county index for block u (same semantics as in
    /// `equalize_exact`).
    fn build_equalization_graph(&self, series: &str, county_ids: &[u32]) -> EqualizationGraph {
        use std::collections::{HashMap, HashSet};

        let n = (self.num_parts() - 1) as usize; // real districts (excluding unassigned 0)

        // Compute T (base_target) and r (num_extra).
        let total_pop: u64 = (1..self.num_parts())
            .map(|p| self.part_weights().get_as_f64(series, p as usize).unwrap_or(0.0).round() as u64)
            .sum();
        let base_target = total_pop / n as u64;
        let num_extra = (total_pop % n as u64) as usize; // r

        // Current population of each real district.  pops[i] = district (i+1).
        // Stored in the graph so compute_spanning_tree can build subtree sums.
        let pops: Vec<i64> = (1..self.num_parts())
            .map(|p| self.part_weights().get_as_f64(series, p as usize).unwrap_or(0.0).round() as i64)
            .collect();

        // ---- Collect edges: scan every block-level adjacency ----------------
        // For each directed edge u→v (u in district a, v in district b, a ≠ b):
        //   - skip if either district is unassigned (0)
        //   - skip if county_ids[u] ≠ county_ids[v] (county boundary — swapping
        //     across it would create a new county split)
        // For each surviving pair, record u in frontier_a and v in frontier_b
        // (using HashSets to avoid duplicates from multiple valid neighbours).
        let mut frontier_sets: HashMap<(u32, u32), (HashSet<usize>, HashSet<usize>)> =
            HashMap::new();

        for u in 0..self.num_nodes() {
            let pa = self.assignment(u);
            if pa == 0 { continue; }
            for v in self.graph().edges(u) {
                let pb = self.assignment(v);
                if pb == 0 || pb == pa { continue; }
                if county_ids[u] != county_ids[v] { continue; } // county boundary — skip

                // Normalise so a < b.
                let (a, b) = if pa < pb { (pa, pb) } else { (pb, pa) };
                let entry = frontier_sets.entry((a, b)).or_default();
                // u is in district pa; add it to whichever side of (a,b) pa belongs to.
                if pa == a { entry.0.insert(u); } else { entry.1.insert(u); }
            }
        }

        // ---- Build edge list and adjacency index ----------------------------
        let mut edges: Vec<EqualizationEdge> = Vec::with_capacity(frontier_sets.len());
        // adj[d] = indices into `edges` for every edge incident to district d.
        let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n + 1];

        for ((a, b), (fa, fb)) in frontier_sets {
            let idx = edges.len();
            adj[a as usize].push(idx);
            adj[b as usize].push(idx);
            edges.push(EqualizationEdge {
                a, b,
                frontier_a: fa.into_iter().collect(),
                frontier_b: fb.into_iter().collect(),
            });
        }

        EqualizationGraph { base_target, num_extra, pops, edges, adj }
    }

    // -------------------------------------------------------------------------
    // Phase 2 helpers — per-edge transfer menus
    // -------------------------------------------------------------------------

    /// Precompute the transfer menu for one edge of the equalization graph.
    ///
    /// **Tree edges** (`tree.is_tree_edge(edge_idx)`): iterates over all
    /// feasible k values in `tree.subtree_k_range(graph, child)` to enumerate
    /// distinct net_flows via `tree.tree_edge_net_flow`.  Solves
    /// `solve_transfer_ilp` for each and stores results keyed by net_flow.
    ///
    /// **Cycle edges**: solves only net_flow = 0 (no default transfer).
    /// Build the list of transferable candidate blocks for one side of an edge.
    ///
    /// Starts with the immediate frontier (blocks in `from_district` adjacent to
    /// `to_district`), filtered to those that are individually safe to remove.
    /// Candidates are BFS-ordered within the frontier so the knapsack DP
    /// reconstruction tends to pick spatially adjacent blocks.
    ///
    /// If the total population of the immediate frontier is less than
    /// `min_required`, the BFS continues into the district interior (depth ≥ 2
    /// blocks adjacent to already-included blocks) until the required population
    /// is reached or no more safe-to-remove blocks are available.  Interior
    /// blocks are added as *singleton* items; the contiguity check in
    /// `solve_transfer_ilp` handles removal of mixed frontier/interior sets.
    fn build_candidates(
        &mut self,
        series: &str,
        frontier: &[usize],
        from_district: u32,
        to_district: u32,
        min_required: u64,
    ) -> Vec<TransferableRegion> {
        // Collect the frontier into a set for fast lookup.
        let frontier_set: HashSet<usize> = frontier.iter().copied().collect();

        // BFS-order the frontier blocks: BFS along edges where BOTH endpoints
        // are in the frontier.  This spatial ordering makes the DP reconstruction
        // select geographically adjacent (and thus more likely contiguous) subsets.
        let mut bfs_ordered: Vec<usize> = Vec::with_capacity(frontier.len());
        {
            let mut visited: HashSet<usize> = HashSet::with_capacity(frontier.len());
            for &start in frontier {
                if visited.contains(&start) { continue; }
                let mut queue = VecDeque::from([start]);
                visited.insert(start);
                while let Some(u) = queue.pop_front() {
                    bfs_ordered.push(u);
                    for v in self.unit_graph.edges(u) {
                        if frontier_set.contains(&v) && visited.insert(v) {
                            queue.push_back(v);
                        }
                    }
                }
            }
        }

        // Filter frontier to blocks individually safe to remove.
        let mut candidates: Vec<TransferableRegion> = Vec::new();
        let mut in_candidates: HashSet<usize> = HashSet::new();
        let mut total_pop: u64 = 0;
        for &u in &bfs_ordered {
            if !self.check_node_contiguity(u, to_district) { continue; }
            let pop = self.unit_weights().get_as_f64(series, u).unwrap_or(0.0).round() as u64;
            candidates.push(TransferableRegion { nodes: vec![u], population: pop });
            in_candidates.insert(u);
            total_pop += pop;
        }

        // If the immediate frontier doesn't have enough population, expand into
        // the district interior via BFS.  Each interior block is added only if
        // it's individually safe to remove (not an articulation point for the
        // district).  We use `check_node_contiguity(u, from_district)` — but
        // with the destination set to 0 (unassigned) to check if removing u
        // from `from_district` leaves it connected.
        if total_pop < min_required {
            let mut expand_queue: VecDeque<usize> = candidates.iter()
                .map(|r| r.nodes[0])
                .collect();
            let mut visited_expansion: HashSet<usize> = in_candidates.clone();

            'expand: while let Some(u) = expand_queue.pop_front() {
                // Collect neighbors first (immutable borrow), then check/mutate.
                let neighbors: Vec<usize> = self.unit_graph.edges(u)
                    .filter(|&v| self.assignment(v) == from_district
                              && !visited_expansion.contains(&v))
                    .collect();
                for v in neighbors {
                    visited_expansion.insert(v);
                    // Check if we can safely remove v from from_district.
                    if !self.check_node_contiguity(v, 0) { continue; }
                    let pop = self.unit_weights().get_as_f64(series, v).unwrap_or(0.0).round() as u64;
                    candidates.push(TransferableRegion { nodes: vec![v], population: pop });
                    in_candidates.insert(v);
                    total_pop += pop;
                    expand_queue.push_back(v);
                    if total_pop >= min_required { break 'expand; }
                }
            }
        }

        candidates
    }

    /// Non-zero flows on cycle edges are solved on demand by Phase 3 when
    /// rerouting and added to the menu at that point.
    ///
    /// Starts at search radius max_k = 1 (immediate frontier only) and retries
    /// at increasing k if the ILP finds no solution for a given net_flow.
    fn precompute_edge_menu(
        &mut self,
        edge_idx: usize,
        graph: &EqualizationGraph,
        tree: &SpanningTree,
        series: &str,
    ) -> EdgeMenu {
        let mut solutions: std::collections::HashMap<i64, Option<Vec<Transfer>>> =
            std::collections::HashMap::new();

        if !tree.is_tree_edge(edge_idx) {
            // Cycle edges carry zero flow by default; ILP trivially empty.
            solutions.insert(0, Some(vec![]));
            return EdgeMenu { solutions };
        }

        let e_a = graph.edges[edge_idx].a;
        let e_b = graph.edges[edge_idx].b;
        let flows = tree.tree_edge_net_flows[edge_idx].clone();

        // Use the frontier blocks already computed by build_equalization_graph.
        // Clone them so we can use &mut self for the contiguity checks below.
        let raw_a: Vec<usize> = graph.edges[edge_idx].frontier_a.clone();
        let raw_b: Vec<usize> = graph.edges[edge_idx].frontier_b.clone();

        // Determine the maximum required flow magnitude for this edge,
        // so we know how many candidates are needed.
        let max_flow: u64 = flows.iter().map(|f| f.unsigned_abs() as u64).max().unwrap_or(0);

        let candidates_a = self.build_candidates(series, &raw_a, e_a, e_b, max_flow);
        let candidates_b = self.build_candidates(series, &raw_b, e_b, e_a, max_flow);

        println!("  [ei={edge_idx}] raw_a={} cand_a={} raw_b={} cand_b={}",
            raw_a.len(), candidates_a.len(), raw_b.len(), candidates_b.len());

        for flow in flows {
            if flow == 0 {
                solutions.entry(0).or_insert(Some(vec![]));
                continue;
            }
            let result = self.solve_transfer_ilp(&candidates_a, &candidates_b, e_a, e_b, flow);
            solutions.insert(flow, result);
        }

        EdgeMenu { solutions }
    }

    /// Enumerate all "transferable regions" on the boundary between districts
    /// `from` and `to`: connected subgraphs of `from` that contain at least one
    /// block adjacent to `to`, whose removal leaves `from` contiguous, and
    /// whose blocks are closer to the (from, to) boundary than to any other
    /// district boundary (to prevent conflicts when merging solutions later).
    ///
    /// `max_k` is the maximum graph-distance from the immediate frontier to
    /// include.  Start with max_k = 1 (immediate frontier only) and increase
    /// only if the ILP finds no solution at the current radius.
    fn enumerate_transferable_regions(
        &mut self,
        from: u32,
        to: u32,
        _max_k: usize,
        series: &str,
    ) -> Vec<TransferableRegion> {
        // Basic implementation (max_k=1): singleton frontier blocks that can be
        // safely moved without breaking contiguity of `from`.
        //
        // Collect frontier nodes first (immutable pass), then check contiguity
        // (requires &mut self for union-find path compression).
        let frontier: Vec<usize> = (0..self.num_nodes())
            .filter(|&u| {
                self.assignment(u) == from
                    && self.graph().edges(u).any(|v| self.assignment(v) == to)
            })
            .collect();

        let mut regions = Vec::new();
        for u in frontier {
            if !self.check_node_contiguity(u, to) { continue; }
            let population = self.unit_weights()
                .get_as_f64(series, u)
                .unwrap_or(0.0)
                .round() as u64;
            regions.push(TransferableRegion { nodes: vec![u], population });
        }
        regions
    }

    /// Branch-and-bound solver for the local ILP on a single boundary.
    ///
    /// Given a set of pre-validated transferable regions (all on the same side
    /// of a boundary, moving from `from` to `to`) and a target net population
    /// transfer amount, find the minimum-cardinality subset of regions whose
    /// combined population equals `target_pop`.  Returns the selected transfers,
    /// or None if no feasible subset exists.
    ///
    /// `target_pop` is always positive: the caller is responsible for choosing
    /// the correct side's candidates and passing the absolute flow amount.
    ///
    /// Regions are sorted by population ascending so the search finds small
    /// solutions first and terminates at the first feasible leaf.
    /// Solve the per-edge transfer ILP: find subsets of frontier blocks on both
    /// sides of a boundary whose signed population sum equals `target_flow`.
    ///
    /// Blocks in `candidates_a` move from district `a` to `b`, contributing
    /// +pop to the net flow.  Blocks in `candidates_b` move from `b` to `a`,
    /// contributing −pop.  The ILP finds a combination summing to `target_flow`
    /// (positive = net a→b, negative = net b→a).
    ///
    /// This formulation is strictly more powerful than one-directional transfer:
    /// if no unidirectional subset achieves the target, a mix of a→b and b→a
    /// blocks may still do so (e.g. target=+5, achieved by moving 8 people a→b
    /// and 3 people b→a simultaneously).
    ///
    /// **Contiguity assumption**: removing S_a from `a` must leave `a` contiguous
    /// (ignoring S_b being added); likewise removing S_b from `b` must leave `b`
    /// contiguous (ignoring S_a).  This is stricter than the true condition but
    /// allows the two checks to be independent: since adding nodes to a district
    /// can never disconnect it, if removal-only is contiguous then the full move
    /// (with additions) is also contiguous.
    ///
    /// Individual candidates are pre-filtered by `enumerate_transferable_regions`
    /// (per-block articulation check).  After the search finds a candidate
    /// solution, `check_subgraph_contiguity` verifies the combined removal set
    /// for each direction.  If either check fails, `None` is returned; the caller
    /// may fall back to a cycle-edge reroute (Phase 3).
    fn solve_transfer_ilp(
        &self,
        candidates_a: &[TransferableRegion],
        candidates_b: &[TransferableRegion],
        a: u32,
        b: u32,
        target_flow: i64,
    ) -> Option<Vec<Transfer>> {
        if target_flow == 0 {
            return Some(vec![]);
        }

        // Layer 1 — single-block solution.
        // A-side alone:  pop_a == target  (only possible when target > 0).
        // B-side alone:  pop_b == -target (only possible when target < 0).
        if target_flow > 0 {
            let need = target_flow as u64;
            if let Some(r) = candidates_a.iter().find(|r| r.population == need) {
                return Some(vec![Transfer { from: a, to: b, nodes: r.nodes.clone() }]);
            }
        } else {
            let need = (-target_flow) as u64;
            if let Some(r) = candidates_b.iter().find(|r| r.population == need) {
                return Some(vec![Transfer { from: b, to: a, nodes: r.nodes.clone() }]);
            }
        }

        // Layer 2 — one block from each side.
        // net = pop_a − pop_b = target_flow  ⟹  pop_b = pop_a − target_flow.
        // Build a hash map from b-side populations to candidate indices for O(n) lookup.
        {
            use std::collections::HashMap;
            let b_by_pop: HashMap<u64, usize> = candidates_b.iter().enumerate()
                .filter(|(_, r)| r.population > 0)
                .map(|(i, r)| (r.population, i))
                .collect();

            for r_a in candidates_a.iter().filter(|r| r.population > 0) {
                let need_b = r_a.population as i64 - target_flow;
                if need_b > 0 {
                    if let Some(&bi) = b_by_pop.get(&(need_b as u64)) {
                        let r_b = &candidates_b[bi];
                        // Each block individually passed check_node_contiguity, so no
                        // further collective check is needed for single-block-per-side moves.
                        return Some(vec![
                            Transfer { from: a, to: b, nodes: r_a.nodes.clone() },
                            Transfer { from: b, to: a, nodes: r_b.nodes.clone() },
                        ]);
                    }
                }
            }
        }

        // Layer 2.5 — two-knapsack DP for large targets.
        //
        // For |target| > LAYER25_THRESHOLD, build 0/1 knapsack DP tables over
        // the primary and secondary candidate sets separately, then find a pair
        // (primary_sum, secondary_sum) with primary_sum − secondary_sum = abs_target
        // such that removing the chosen blocks from each side is contiguous.
        //
        // Attempt A: primary_sum = abs_target (no counter-flow).
        // Attempt B: iterate over secondary_sum = 1, 2, ... (small counter-flow
        //   amounts).  Each secondary_sum gives a different primary_sum and hence
        //   a different primary subset, providing variety for contiguity.
        //
        // Unlike the signed B&B (Layer 3), this never includes negative items in
        // the same search, so it never "bounces" on large targets.
        {
            const LAYER25_THRESHOLD: u64 = 100;
            let abs_target = target_flow.unsigned_abs();
            if abs_target > LAYER25_THRESHOLD {
                let (pri_cands, sec_cands, from_d, to_d) = if target_flow > 0 {
                    (candidates_a, candidates_b, a, b)
                } else {
                    (candidates_b, candidates_a, b, a)
                };

                let pri_items: Vec<(u64, usize)> = pri_cands.iter().enumerate()
                    .filter(|(_, r)| r.population > 0)
                    .map(|(i, r)| (r.population, i))
                    .collect();
                let sec_items: Vec<(u64, usize)> = sec_cands.iter().enumerate()
                    .filter(|(_, r)| r.population > 0)
                    .map(|(i, r)| (r.population, i))
                    .collect();

                let total_pri: u64 = pri_items.iter().map(|(p, _)| *p).sum();
                let total_sec: u64 = sec_items.iter().map(|(p, _)| *p).sum();

                if total_pri >= abs_target {
                    // Cap the DP table to avoid excessive memory usage.
                    const MAX_DP_CAP: u64 = 500_000;
                    let max_cap = abs_target.saturating_add(total_sec)
                        .min(total_pri).min(MAX_DP_CAP) as usize;

                    let pri_dp = dp_knapsack(&pri_items, max_cap);

                    // --- Attempt A: pure primary (no counter-flow) ---
                    if let Some(chosen) = dp_reconstruct(&pri_dp, &pri_items, abs_target as usize) {
                        let nodes: Vec<usize> = chosen.iter()
                            .flat_map(|&i| pri_cands[i].nodes.iter().copied())
                            .collect();
                        if nodes.len() <= 1 || self.contiguous_after_removal(from_d, &nodes) {
                            return Some(vec![Transfer { from: from_d, to: to_d, nodes }]);
                        }
                    }

                    // --- Attempt B: two-knapsack (primary overshoot + counter-flow) ---
                    if total_sec > 0 {
                        let sec_max = total_sec as usize;
                        let sec_dp  = dp_knapsack(&sec_items, sec_max);

                        let mut pairs_tried = 0usize;
                        for sec_sum in 1..=sec_max {
                            if sec_dp[sec_sum].is_none() { continue; }
                            let pri_sum = abs_target as usize + sec_sum;
                            if pri_sum > max_cap { break; }
                            if pri_dp[pri_sum].is_none() { continue; }

                            pairs_tried += 1;
                            if pairs_tried > 200 { break; }

                            let pri_chosen = dp_reconstruct(&pri_dp, &pri_items, pri_sum).unwrap();
                            let sec_chosen = dp_reconstruct(&sec_dp, &sec_items, sec_sum).unwrap();

                            let pri_nodes: Vec<usize> = pri_chosen.iter()
                                .flat_map(|&i| pri_cands[i].nodes.iter().copied())
                                .collect();
                            let sec_nodes: Vec<usize> = sec_chosen.iter()
                                .flat_map(|&i| sec_cands[i].nodes.iter().copied())
                                .collect();

                            let pri_ok = pri_nodes.len() <= 1
                                || self.contiguous_after_removal(from_d, &pri_nodes);
                            let sec_ok = sec_nodes.len() <= 1
                                || self.contiguous_after_removal(to_d, &sec_nodes);

                            if pri_ok && sec_ok {
                                let mut transfers = vec![
                                    Transfer { from: from_d, to: to_d, nodes: pri_nodes },
                                ];
                                transfers.push(Transfer { from: to_d, to: from_d, nodes: sec_nodes });
                                return Some(transfers);
                            }
                        }
                    }
                }
                // Fall through to Layer 3.
            }
        }

        // Layer 3 — branch-and-bound for 3+ block combinations.
        //
        // Sort order is chosen by target magnitude:
        //   Small target  → ascending  |val|: a few small blocks reach it quickly.
        //   Large target  → descending |val|: a few large blocks reach it quickly.
        //
        // A per-call node budget caps worst-case search time.
        {
            let mut items: Vec<(i64, usize, bool)> = Vec::new();
            for (i, r) in candidates_a.iter().enumerate() {
                if r.population > 0 { items.push((r.population as i64, i, true)); }
            }
            for (i, r) in candidates_b.iter().enumerate() {
                if r.population > 0 { items.push((-(r.population as i64), i, false)); }
            }
            if items.is_empty() { return None; }

            const LARGE_TARGET_THRESHOLD: u64 = 50;
            if target_flow.unsigned_abs() <= LARGE_TARGET_THRESHOLD {
                items.sort_by_key(|&(v, _, _)| v.unsigned_abs());           // ascending
            } else {
                items.sort_by_key(|&(v, _, _)| std::cmp::Reverse(v.unsigned_abs())); // descending
            }

            let n = items.len();
            let mut suffix_max = vec![0i64; n + 1];
            let mut suffix_min = vec![0i64; n + 1];
            for i in (0..n).rev() {
                let v = items[i].0;
                suffix_max[i] = suffix_max[i + 1] + v.max(0);
                suffix_min[i] = suffix_min[i + 1] + v.min(0);
            }
            if target_flow > suffix_max[0] || target_flow < suffix_min[0] { return None; }

            let mut chosen: Vec<(usize, bool)> = Vec::new();
            let mut budget = 200_000usize;
            if !signed_subset_sum_search(&items, &suffix_max, &suffix_min, 0, target_flow, &mut chosen, &mut budget) {
                return None;
            }

            let ab_nodes: Vec<usize> = chosen.iter().filter(|(_, is_a)| *is_a)
                .flat_map(|(i, _)| candidates_a[*i].nodes.iter().copied()).collect();
            let ba_nodes: Vec<usize> = chosen.iter().filter(|(_, is_a)| !*is_a)
                .flat_map(|(i, _)| candidates_b[*i].nodes.iter().copied()).collect();

            // Collective contiguity check for multi-block removals.
            if ab_nodes.len() > 1 && !self.contiguous_after_removal(a, &ab_nodes) { return None; }
            if ba_nodes.len() > 1 && !self.contiguous_after_removal(b, &ba_nodes) { return None; }

            let mut transfers = Vec::new();
            if !ab_nodes.is_empty() { transfers.push(Transfer { from: a, to: b, nodes: ab_nodes }); }
            if !ba_nodes.is_empty() { transfers.push(Transfer { from: b, to: a, nodes: ba_nodes }); }
            Some(transfers)
        }
    }

    // -------------------------------------------------------------------------
    // Phase 3 helper — global excess assignment
    // -------------------------------------------------------------------------

    /// Branch-and-bound over excess assignments.
    ///
    /// Find a globally consistent assignment of exactly r of the N districts
    /// to the T+1 target (the remaining N−r get T) such that every tree edge's
    /// implied net_flow has a feasible ILP solution in its EdgeMenu.
    ///
    /// The search proceeds depth-first along the spanning tree (root to leaves),
    /// computing each tree edge's net_flow via `SpanningTree::tree_edge_net_flow`
    /// and pruning as soon as any menu lookup returns None.
    ///
    /// When the spanning tree solution is infeasible for some edge despite all
    /// excess assignments being tried, the function attempts to reroute flow
    /// through an adjacent cycle edge: it solves the ILP for that cycle edge
    /// with the computed rerouting amount (reducing the failing tree edge's
    /// load), caches the result in `menus`, and resumes the search.
    ///
    /// On success, returns a `Vec<i64>` of length `graph.edges.len()` where
    /// `result[ei]` is the chosen net_flow for edge `ei` (0 for edges with no
    /// transfer).  Phase 4 uses this to look up the correct EdgeMenu solution
    /// for each edge.  Returns None if the problem is infeasible, otherwise
    /// returns `(flows, fallback_edge_count)`.
    fn solve_excess_assignment(
        graph: &EqualizationGraph,
        tree: &SpanningTree,
        menus: &mut std::collections::HashMap<usize, EdgeMenu>,
        _series: &str,
    ) -> Option<(Vec<i64>, usize)> {
        let n = graph.pops.len();
        let r = graph.num_extra;

        // ── Build parent→children adjacency for top-down traversal ──────────
        let mut children: Vec<Vec<(u32, usize)>> = vec![Vec::new(); n + 1];
        for &ei in &tree.tree_edges {
            let edge  = &graph.edges[ei];
            let child  = if tree.tree_parent_edge[edge.b as usize] == Some(ei) { edge.b } else { edge.a };
            let parent = tree.tree_parent[child as usize].unwrap();
            children[parent as usize].push((child, ei));
        }

        // ── Forward pass (leaves → root): compute feasible_k[ei] ────────────
        // feasible_k[ei] = sorted list of k values (# T+1 districts in the
        // child subtree) for which:
        //   (a) the implied net_flow is in menus[ei] with a Some solution, AND
        //   (b) a valid sub-assignment exists for the entire subtree below.
        let mut feasible_k: Vec<Vec<usize>> = vec![Vec::new(); graph.edges.len()];

        for &ei in tree.tree_edges.iter().rev() { // rev() → leaf-to-root order
            let edge  = &graph.edges[ei];
            let child = if tree.tree_parent_edge[edge.b as usize] == Some(ei) { edge.b } else { edge.a };
            let k_range    = tree.subtree_k_range(graph, ei);
            let child_eids: Vec<usize> = children[child as usize].iter().map(|&(_, ci)| ci).collect();

            'k_loop: for k in k_range {
                // (a) Local menu check.
                let flow = tree.tree_edge_net_flow(graph, ei, k);
                let menu_ok = menus.get(&ei)
                    .and_then(|m| m.solutions.get(&flow))
                    .map(|s| s.is_some())
                    .unwrap_or(false);
                if !menu_ok { continue; }

                // (b) Subtree check: children's feasible k values must be able
                //     to sum to (k − k_d) for some k_d ∈ {0, 1}.
                if child_eids.is_empty() {
                    // Leaf district: subtree contains only the district itself.
                    if k <= 1 { feasible_k[ei].push(k); }
                } else {
                    for k_d in 0..=1usize {
                        if k < k_d { continue; }
                        if children_can_sum(&child_eids, &feasible_k, k - k_d) {
                            feasible_k[ei].push(k);
                            continue 'k_loop;
                        }
                    }
                }
            }
        }

        // ── Fallback: edges with no ILP-feasible k default to no change ──────
        // If any edge's feasible_k is empty (its ILP had no solution for any
        // required flow), allow all k values in its range.  The backward pass
        // will output flow=0 for these edges, leaving the boundary unchanged.
        let mut fallback_edges: Vec<usize> = Vec::new();
        for &ei in &tree.tree_edges {
            if feasible_k[ei].is_empty() {
                let edge = &graph.edges[ei];
                println!("  [DP] ei={ei} d{}↔d{}: no ILP solution — defaulting to no change",
                    edge.a, edge.b);
                feasible_k[ei].extend(tree.subtree_k_range(graph, ei));
                fallback_edges.push(ei);
            }
        }

        // ── Root feasibility check ───────────────────────────────────────────
        let root_child_eids: Vec<usize> = children[1].iter().map(|&(_, ci)| ci).collect();
        let k_root = (0..=r.min(1))
            .find(|&kr| children_can_sum(&root_child_eids, &feasible_k, r - kr))?;

        // ── Backward pass (root → leaves): reconstruct assignment ────────────
        // For each district, given its subtree's k value (fixed by the parent
        // edge choice), pick k_d ∈ {0,1} and assign k values to child edges,
        // preferring choices that yield zero net_flow (= no block moves).
        let mut chosen_k: Vec<usize> = vec![0; graph.edges.len()];

        // Stack: (district, k_subtree).  Start with root's children.
        let mut stack: Vec<(u32, usize)> = Vec::new();
        let root_assignment = assign_children_preferred(
            &root_child_eids, &feasible_k, tree, graph, r - k_root,
        )?;
        for (&(child_d, child_ei), &k) in children[1].iter().zip(root_assignment.iter()) {
            chosen_k[child_ei] = k;
            stack.push((child_d, k));
        }

        while let Some((d, k_subtree)) = stack.pop() {
            let child_list = &children[d as usize];
            if child_list.is_empty() { continue; }

            let child_eids: Vec<usize> = child_list.iter().map(|&(_, ei)| ei).collect();

            // Try k_d = 0 first (prefer not consuming a T+1 slot at this
            // district unless needed), then k_d = 1.
            let mut assigned = false;
            for k_d in 0..=1usize {
                if k_subtree < k_d { continue; }
                if let Some(assignment) = assign_children_preferred(
                    &child_eids, &feasible_k, tree, graph, k_subtree - k_d,
                ) {
                    for (&(child_d, child_ei), &k) in child_list.iter().zip(assignment.iter()) {
                        chosen_k[child_ei] = k;
                        stack.push((child_d, k));
                    }
                    assigned = true;
                    break;
                }
            }

            if !assigned {
                // The forward pass guaranteed feasibility — this is a bug.
                return None;
            }
        }
        let fallback_count = fallback_edges.len();

        // ── Convert chosen k values to net_flows ─────────────────────────────
        // Only emit a non-zero flow when the menu has a confirmed Some solution.
        // Fallback edges (no ILP solution) stay at 0 — no blocks are moved.
        let mut flows = vec![0i64; graph.edges.len()];
        for &ei in &tree.tree_edges {
            let flow = tree.tree_edge_net_flow(graph, ei, chosen_k[ei]);
            let has_solution = menus.get(&ei)
                .and_then(|m| m.solutions.get(&flow))
                .map(|s| s.is_some())
                .unwrap_or(false);
            if has_solution {
                flows[ei] = flow;
            }
        }

        // ── Enforce fallback invariant: no flow for edges that had no feasible k ─
        // The forward pass allows any k for fallback edges, and the backward pass
        // may accidentally pick a k whose flow has a Some solution in the menu.
        // Executing such flows is unsafe — they may not be globally consistent
        // (the subtree constraint that caused the fallback was not satisfied).
        for &ei in &fallback_edges {
            flows[ei] = 0;
        }

        // ── Safety filter: prevent harmful partial execution ─────────────────
        // When upstream edges fail (flow=0), "pass-through" districts give
        // population to their children without receiving from their parents,
        // ending up MORE deficit.  Only execute a transfer from src to dst if
        // src would remain ≥ T after the transfer, OR src's parent edge (the
        // edge connecting src to its parent in the spanning tree) is also
        // non-zero — indicating src will receive compensating inflow.
        {
            let t = graph.base_target as i64;
            for &ei in &tree.tree_edges {
                if flows[ei] == 0 { continue; }
                let edge = &graph.edges[ei];
                let flow = flows[ei];
                let (src, dst_idx) = if flow > 0 {
                    (edge.a, edge.b as usize - 1)
                } else {
                    (edge.b, edge.a as usize - 1)
                };
                let src_pop = graph.pops[src as usize - 1];
                let dst_pop = graph.pops[dst_idx];
                let amount = flow.unsigned_abs() as i64;

                // If src is surplus and dst is deficit: always safe.
                if src_pop > t && dst_pop < t { continue; }

                // If src would become more deficit (src loses pop and was already <= T):
                // only allow if src's parent edge also has non-zero flow (src will be compensated).
                if src_pop - amount < t {
                    let parent_ei_opt = tree.tree_parent_edge[src as usize];
                    let parent_active = parent_ei_opt
                        .map(|pei| flows[pei] != 0)
                        .unwrap_or(false);
                    if !parent_active {
                        flows[ei] = 0;
                        continue;
                    }
                }

                // If dst would become more surplus (dst gains pop and was already >= T):
                // only allow if dst's parent edge has non-zero flow (dst will pass the excess on).
                if dst_pop + amount > t {
                    let parent_ei_opt = tree.tree_parent_edge[dst_idx + 1]; // districts are 1-indexed
                    let parent_active = parent_ei_opt
                        .map(|pei| flows[pei] != 0)
                        .unwrap_or(false);
                    if !parent_active {
                        flows[ei] = 0;
                    }
                }
            }
        }

        // ── Print accepted swaps ──────────────────────────────────────────────
        println!("Accepted swaps:");
        let mut any = false;
        for &ei in &tree.tree_edges {
            let flow = flows[ei];
            if flow == 0 { continue; }
            any = true;
            let edge = &graph.edges[ei];
            let (src, dst) = if flow > 0 { (edge.a, edge.b) } else { (edge.b, edge.a) };
            let people = flow.unsigned_abs();
            println!("  edge {ei}: district {src} → district {dst}, net_flow = {flow:+} ({people} people)");
            if let Some(Some(transfers)) = menus.get(&ei).and_then(|m| m.solutions.get(&flow)) {
                for t in transfers {
                    println!("    move {} blocks ({} nodes) from district {} to district {}",
                        t.nodes.len(), t.nodes.len(), t.from, t.to);
                }
            }
        }
        if !any {
            println!("  (none — all districts already at target population)");
        }

        Some((flows, fallback_count))
    }

    // -------------------------------------------------------------------------
    // Phase 4 helper — apply transfers
    // -------------------------------------------------------------------------

    /// Apply a list of block transfers to the partition.
    ///
    /// For each `Transfer`, move all blocks in `nodes` from district `from`
    /// to district `to`, updating assignment, frontier, and weight caches.
    ///
    /// Transfers are applied in an order that preserves contiguity at every
    /// step: within a single edge, regions are moved smallest-first; across
    /// edges, the ordering follows the flow paths (upstream transfers first).
    /// Apply a list of block transfers, first verifying joint contiguity.
    ///
    /// A district may appear as the source in multiple transfers (from different
    /// tree edges).  The per-edge ILP only checks each edge's removal in
    /// isolation; combining them can disconnect the district.  This function
    /// groups transfers by source district, verifies the combined removal is
    /// contiguous, and greedily drops transfers that would break it (keeping
    /// the largest ones first).  Returns the number of blocks actually moved.
    fn apply_exact_transfers(&mut self, transfers: &[Transfer]) -> usize {
        use std::collections::{HashMap, HashSet};

        // Phase 4a: compute which transfers to skip due to joint non-contiguity.
        let skip: HashSet<usize> = {
            let mut by_from: HashMap<u32, Vec<usize>> = HashMap::new();
            for (ti, t) in transfers.iter().enumerate() {
                by_from.entry(t.from).or_default().push(ti);
            }

            let mut skip: HashSet<usize> = HashSet::new();
            for (dist, mut tidxs) in by_from {
                if tidxs.len() <= 1 { continue; }

                // Check whether all removals from `dist` are jointly contiguous.
                let all_nodes: Vec<usize> = tidxs.iter()
                    .flat_map(|&ti| transfers[ti].nodes.iter().copied())
                    .collect();

                if self.contiguous_after_removal(dist, &all_nodes) { continue; }

                // Joint removal is non-contiguous.  Greedily include transfers
                // largest-first, dropping any that break joint contiguity.
                tidxs.sort_by_key(|&ti| std::cmp::Reverse(transfers[ti].nodes.len()));
                let mut kept_nodes: Vec<usize> = Vec::new();

                for &ti in &tidxs {
                    let test: Vec<usize> = kept_nodes.iter()
                        .chain(transfers[ti].nodes.iter())
                        .copied()
                        .collect();
                    if self.contiguous_after_removal(dist, &test) {
                        kept_nodes.extend_from_slice(&transfers[ti].nodes);
                    } else {
                        println!("[apply] skipping transfer d{}→d{} — joint contiguity conflict",
                            transfers[ti].from, transfers[ti].to);
                        skip.insert(ti);
                    }
                }
            }
            skip
        };

        // Phase 4b: apply the surviving transfers.
        let mut moved = 0;
        for (ti, t) in transfers.iter().enumerate() {
            if skip.contains(&ti) { continue; }
            moved += t.nodes.len();
            self.move_subgraph(&t.nodes, t.to, false);
        }
        moved
    }
}

// =============================================================================
// Free helpers
// =============================================================================

/// Branch-and-bound signed subset-sum search.
///
/// `items[i] = (signed_pop, original_candidate_index, is_a_side)`.
/// `suffix_max[i]` = max achievable sum from items[i..] (include all positives).
/// `suffix_min[i]` = min achievable sum from items[i..] (include all negatives).
/// `remaining` = how much more signed value is needed to hit the target.
/// Appends `(original_candidate_index, is_a_side)` to `chosen` on success.
/// Returns `true` if a solution was found.
fn signed_subset_sum_search(
    items: &[(i64, usize, bool)],
    suffix_max: &[i64],
    suffix_min: &[i64],
    idx: usize,
    remaining: i64,
    chosen: &mut Vec<(usize, bool)>,
    budget: &mut usize,
) -> bool {
    if remaining == 0 { return true; }
    if idx >= items.len() { return false; }
    if *budget == 0 { return false; }
    *budget -= 1;
    // Prune: the target is unreachable from this position.
    if remaining > suffix_max[idx] || remaining < suffix_min[idx] { return false; }

    let (val, orig_idx, is_a) = items[idx];

    // Include items[idx].
    chosen.push((orig_idx, is_a));
    if signed_subset_sum_search(items, suffix_max, suffix_min, idx + 1, remaining - val, chosen, budget) {
        return true;
    }
    chosen.pop();

    // Exclude items[idx].
    signed_subset_sum_search(items, suffix_max, suffix_min, idx + 1, remaining, chosen, budget)
}

/// Build a 0/1 knapsack DP table over `items[i] = (population, list_index)`.
///
/// Returns `dp` where:
///   - `dp[0]  = Some(usize::MAX)` — base-case sentinel (sum 0, no items used)
///   - `dp[v]  = Some(i)` — list-index `i` was the last item added to reach sum `v`
///   - `dp[v]  = None`   — sum `v` is unreachable
///
/// Items with `population == 0` or `population > max_cap` are skipped.
fn dp_knapsack(items: &[(u64, usize)], max_cap: usize) -> Vec<Option<usize>> {
    let mut dp: Vec<Option<usize>> = vec![None; max_cap + 1];
    dp[0] = Some(usize::MAX); // sentinel: sum 0 achieved with no items
    for (i, &(pop, _)) in items.iter().enumerate() {
        let p = pop as usize;
        if p == 0 || p > max_cap { continue; }
        for v in (p..=max_cap).rev() {
            if dp[v].is_none() && dp[v - p].is_some() {
                dp[v] = Some(i);
            }
        }
    }
    dp
}

/// Reconstruct one solution from a DP table built by `dp_knapsack`.
///
/// Returns the original candidate indices (`items[i].1`) for items whose
/// combined population equals `target`.  Returns `None` if `target` is
/// out of range or unreachable.
fn dp_reconstruct(dp: &[Option<usize>], items: &[(u64, usize)], target: usize) -> Option<Vec<usize>> {
    if target >= dp.len() || dp[target].is_none() { return None; }
    let mut chosen = Vec::new();
    let mut v = target;
    while v > 0 {
        match dp[v] {
            None => break,
            Some(i) if i == usize::MAX => break,
            Some(i) => {
                chosen.push(items[i].1); // original candidate index
                v -= items[i].0 as usize;
            }
        }
    }
    Some(chosen)
}

/// Returns true if the given child tree-edges can each contribute exactly one
/// value from their `feasible_k` sets such that the values sum to `target`.
///
/// Uses a forward knapsack DP (each child contributes exactly one choice).
fn children_can_sum(child_eids: &[usize], feasible_k: &[Vec<usize>], target: usize) -> bool {
    if child_eids.is_empty() {
        return target == 0;
    }
    // reachable[s] = true if some assignment of the processed children sums to s.
    let mut reachable = vec![false; target + 1];
    reachable[0] = true;
    for &ei in child_eids {
        let fk = &feasible_k[ei];
        if fk.is_empty() { return false; }
        let mut next = vec![false; target + 1];
        for s in 0..=target {
            if !reachable[s] { continue; }
            for &k in fk {
                let ns = s + k;
                if ns <= target { next[ns] = true; }
            }
        }
        reachable = next;
    }
    reachable[target]
}

/// Assign k values to child tree-edges summing to `need`, preferring the
/// zero-flow k value for each edge (to minimise unnecessary block moves).
///
/// Processes children left-to-right with backtracking: tries each child's
/// zero-flow k first, then other feasible values in ascending order.
/// Returns None if no valid assignment exists.
fn assign_children_preferred(
    child_eids: &[usize],
    feasible_k: &[Vec<usize>],
    tree: &SpanningTree,
    graph: &EqualizationGraph,
    need: usize,
) -> Option<Vec<usize>> {
    if child_eids.is_empty() {
        return if need == 0 { Some(vec![]) } else { None };
    }

    let ei   = child_eids[0];
    let rest = &child_eids[1..];

    // Zero-flow k: the k value that makes net_flow == 0 for this edge.
    let zero_k = tree.subtree_k_range(graph, ei)
        .find(|&k| tree.tree_edge_net_flow(graph, ei, k) == 0);

    // Try-order: zero-flow k first, then remaining values ascending.
    let mut k_order: Vec<usize> = feasible_k[ei].clone();
    k_order.sort_by_key(|&k| {
        let is_zero = zero_k.map(|zk| k == zk).unwrap_or(false);
        (if is_zero { 0u8 } else { 1u8 }, k)
    });

    for k in k_order {
        if k > need { continue; }
        if let Some(mut rest_assignment) =
            assign_children_preferred(rest, feasible_k, tree, graph, need - k)
        {
            let mut result = vec![k];
            result.append(&mut rest_assignment);
            return Some(result);
        }
    }

    None
}

// =============================================================================
// Supporting data types for exact equalization
// =============================================================================

// ---------------------------------------------------------------------------
// Phase 1 — equalization graph
// ---------------------------------------------------------------------------

/// Graph of district pairs that are eligible to exchange blocks during exact
/// equalization (i.e., they share a within-county boundary).
///
/// Constructed once by `build_equalization_graph`.  Nodes correspond to
/// districts 1..=N; edges store the k=0 frontier blocks on both sides.
///
/// Does not include flow-routing data.  Call `compute_spanning_tree` to
/// produce a `SpanningTree` that routes flow and enables ILP solving.
struct EqualizationGraph {
    /// Base per-district target: T = floor(total_pop / N).
    base_target: u64,
    /// Number of districts that receive T+1: r = total_pop % N.
    num_extra: usize,
    /// Current population of each real district.  `pops[i]` = district i+1.
    /// Stored here so `compute_spanning_tree` can build subtree sums without
    /// re-querying the partition.
    pops: Vec<i64>,
    /// All edges, in the order they were inserted (a < b for each entry).
    edges: Vec<EqualizationEdge>,
    /// Adjacency index: `adj[d]` is the list of indices into `edges` where
    /// district d appears (as either a or b).
    adj: Vec<Vec<usize>>,
}

impl EqualizationGraph {
    /// Compute a max-weight spanning tree for this equalization graph, rooted
    /// at district 1.  The tree is chosen by Prim's algorithm, weighting each
    /// edge by `|frontier_a| + |frontier_b|` (total immediately-swappable
    /// blocks).  Maximising this weight ensures that flow-carrying tree edges
    /// have the most candidate transferable regions, improving ILP feasibility.
    ///
    /// Returns a `SpanningTree` containing the tree structure and precomputed
    /// subtree statistics.  The tree can be recomputed at any time by calling
    /// this method again (e.g. after the initial tree proves infeasible for
    /// some edge and an alternative routing is desired).
    fn compute_spanning_tree(&self) -> SpanningTree {
        use std::collections::{BinaryHeap, HashSet};

        let n = self.pops.len(); // number of real districts (1..=n)
        let root = 1u32;

        let mut tree_parent:      Vec<Option<u32>>   = vec![None; n + 1];
        let mut tree_parent_edge: Vec<Option<usize>> = vec![None; n + 1];
        let mut tree_edges:       Vec<usize>          = Vec::new();
        let mut tree_edge_flags:  Vec<bool>           = vec![false; self.edges.len()];

        // Prim's algorithm: max-heap keyed by (frontier_capacity, edge_idx, from_district).
        // Ties broken by edge_idx for determinism.
        let mut heap: BinaryHeap<(usize, usize, u32)> = BinaryHeap::new();
        let mut visited = vec![false; n + 1];
        visited[0] = true;           // slot 0 (unassigned district) never enters tree
        visited[root as usize] = true;

        for &ei in &self.adj[root as usize] {
            let cap = self.edges[ei].frontier_a.len() + self.edges[ei].frontier_b.len();
            heap.push((cap, ei, root));
        }

        while let Some((_, ei, from)) = heap.pop() {
            let other = if self.edges[ei].a == from { self.edges[ei].b } else { self.edges[ei].a };
            if visited[other as usize] { continue; }
            visited[other as usize] = true;
            tree_parent[other as usize]      = Some(from);
            tree_parent_edge[other as usize] = Some(ei);
            tree_edges.push(ei);
            tree_edge_flags[ei] = true;

            for &ej in &self.adj[other as usize] {
                let nbr = if self.edges[ej].a == other { self.edges[ej].b } else { self.edges[ej].a };
                if !visited[nbr as usize] {
                    let cap = self.edges[ej].frontier_a.len() + self.edges[ej].frontier_b.len();
                    heap.push((cap, ej, other));
                }
            }
        }

        // ---- Bottom-up subtree accumulation ---------------------------------
        // tree_edges is parent-before-child (Prim's insertion order); reversing
        // gives child-before-parent (leaf-to-root), the correct bottom-up order.
        //
        // subtree_pop[d-1] = Σ pop_i for districts i in subtree(d).
        // subtree_size[d]  = |subtree(d)|.  Index 0 is unused; initialised to 1
        //                    so the root's accumulation starts from 1.
        let mut subtree_pop:  Vec<i64>   = self.pops.clone();
        let mut subtree_size: Vec<usize> = vec![1; n + 1];

        for &ei in tree_edges.iter().rev() {
            let edge = &self.edges[ei];
            // child = the endpoint that was added to the tree by this edge
            // (identified by which endpoint has tree_parent_edge == ei).
            let child  = if tree_parent_edge[edge.b as usize] == Some(ei) { edge.b } else { edge.a };
            let parent = tree_parent[child as usize].unwrap();
            subtree_pop [parent as usize - 1] += subtree_pop [child as usize - 1];
            subtree_size[parent as usize]     += subtree_size[child as usize];
        }

        // ---- Precompute feasible net_flows per edge --------------------------
        // For each tree edge, k (the count of T+1 districts in the child's
        // subtree) ranges over subtree_k_range.  Each k gives one net_flow
        // value; we collect unique values in a→b sign convention (positive
        // = blocks move from district a to district b).
        //
        // For cycle edges the only feasible flow is 0 (no transfer), so we
        // store [0] for every cycle edge so callers can iterate uniformly.
        let n_reachable = subtree_size[root as usize]; // may be < n if graph is disconnected

        let mut tree_edge_net_flows: Vec<Vec<i64>> = vec![vec![0i64]; self.edges.len()];

        for &ei in &tree_edges {
            let edge  = &self.edges[ei];
            let child = if tree_parent_edge[edge.b as usize] == Some(ei) { edge.b } else { edge.a };
            let sz        = subtree_size[child as usize];
            let n_outside = n_reachable - sz;
            let min_k = self.num_extra.saturating_sub(n_outside);
            let max_k = self.num_extra.min(sz);

            let mut flows: Vec<i64> = Vec::with_capacity(max_k - min_k + 1);
            let mut seen: HashSet<i64> = HashSet::new();

            for k in min_k..=max_k {
                // surplus > 0: child's subtree has too much population and
                // must export blocks toward the root (parent side).
                let surplus = subtree_pop[child as usize - 1]
                    - sz as i64 * self.base_target as i64
                    - k as i64;
                // Convert to the a→b sign convention used by EdgeMenu:
                // positive means blocks flow from a to b.  When the child is
                // b, "toward root" is toward a, so we negate.
                let net_flow = if child == edge.a { surplus } else { -surplus };
                if seen.insert(net_flow) {
                    flows.push(net_flow);
                }
            }

            tree_edge_net_flows[ei] = flows;
        }

        SpanningTree {
            tree_edges, tree_edge_flags,
            tree_parent, tree_parent_edge,
            subtree_pop, subtree_size,
            tree_edge_net_flows,
        }
    }
}

/// A spanning tree of the equalization graph, rooted at district 1.
///
/// Produced by `EqualizationGraph::compute_spanning_tree`.  Encapsulates all
/// flow-routing data derived from the chosen tree: parent pointers, subtree
/// population sums, and the net_flow formula.  A new `SpanningTree` can be
/// computed at any time to reroute flow (e.g. if the current tree proves
/// infeasible for some edge).
struct SpanningTree {
    /// Indices into `EqualizationGraph::edges` that are in the tree, in
    /// parent-before-child order (guaranteed by Prim's construction).
    /// Cycle edges are absent.  Reversing this slice gives bottom-up
    /// (leaf-to-root) order, which is used for subtree accumulation.
    tree_edges: Vec<usize>,
    /// `tree_edge_flags[ei]` = true if `edges[ei]` is a tree edge.
    /// Enables O(1) tree-membership tests without a HashSet.
    tree_edge_flags: Vec<bool>,
    /// `tree_parent[d]` = parent district of d in the rooted spanning tree,
    /// or None if d is the root or unreachable.
    tree_parent: Vec<Option<u32>>,
    /// `tree_parent_edge[d]` = index into `edges` of the tree edge connecting
    /// d to its parent, or None for the root.
    tree_parent_edge: Vec<Option<usize>>,
    /// `subtree_pop[d-1]` = Σ pop_i over all districts i in the subtree rooted
    /// at d (including d itself).
    subtree_pop: Vec<i64>,
    /// `subtree_size[d]` = number of districts in the subtree rooted at d.
    /// Index 0 is unused.
    subtree_size: Vec<usize>,
    /// Precomputed feasible net_flows for each edge, in the a→b sign
    /// convention (positive = blocks move from district `a` to district `b`).
    ///
    /// For tree edges, the list contains one entry per distinct net_flow value
    /// across the subtree k-range (at most `r + 1` entries, often fewer after
    /// deduplication).  Phase 2 solves `solve_transfer_ilp` for each entry.
    ///
    /// For cycle edges, the list is always `[0]`: no transfer is required by
    /// default, and non-zero rerouting flows are solved on demand by Phase 3.
    ///
    /// Indexed parallel to `EqualizationGraph::edges`.
    tree_edge_net_flows: Vec<Vec<i64>>,
}

impl SpanningTree {
    /// Whether edge `ei` (an index into `EqualizationGraph::edges`) is a tree
    /// edge in this spanning tree.
    #[inline]
    fn is_tree_edge(&self, ei: usize) -> bool {
        self.tree_edge_flags[ei]
    }

    /// Compute the net_flow on tree edge `ei` given that `k` districts in the
    /// child's subtree receive the T+1 target.
    ///
    /// Returns the flow in the **a→b sign convention**: positive means blocks
    /// move from district `a` to district `b`; negative means b→a.
    ///
    /// The precomputed `tree_edge_net_flows[ei]` already enumerates these
    /// values for all feasible k; call this directly only when computing a
    /// specific k (e.g. during Phase 3 incremental search).
    ///
    /// Panics if `ei` is not a tree edge.
    fn tree_edge_net_flow(&self, graph: &EqualizationGraph, ei: usize, k: usize) -> i64 {
        let edge  = &graph.edges[ei];
        let child = if self.tree_parent_edge[edge.b as usize] == Some(ei) { edge.b } else { edge.a };
        let surplus = self.subtree_pop[child as usize - 1]
            - self.subtree_size[child as usize] as i64 * graph.base_target as i64
            - k as i64;
        // Positive surplus = child's subtree must export population toward root.
        // When child == a, "toward root" = toward b, so surplus is already a→b.
        // When child == b, "toward root" = toward a, so negate to get a→b.
        if child == edge.a { surplus } else { -surplus }
    }

    /// Range of feasible k values (number of T+1 districts in the child's
    /// subtree for tree edge `ei`), given the total excess r.
    ///
    /// The outside of the subtree can absorb at most `n_outside` T+1
    /// districts, so at least `max(0, r − n_outside)` must be inside.
    /// The subtree itself has room for at most `min(r, sz)`.
    ///
    /// Panics if `ei` is not a tree edge.
    fn subtree_k_range(&self, graph: &EqualizationGraph, ei: usize) -> std::ops::RangeInclusive<usize> {
        let edge  = &graph.edges[ei];
        let child = if self.tree_parent_edge[edge.b as usize] == Some(ei) { edge.b } else { edge.a };
        let sz        = self.subtree_size[child as usize];
        let n_outside = self.subtree_size[1] - sz; // subtree_size[1] = total reachable
        let min_k = graph.num_extra.saturating_sub(n_outside);
        let max_k = graph.num_extra.min(sz);
        min_k..=max_k
    }
}

/// One edge of the equalization graph: a pair of districts eligible for block
/// exchange, together with their shared frontier at search radius k=0.
struct EqualizationEdge {
    /// Districts on either side, with a < b.
    a: u32,
    b: u32,
    /// Blocks in district a that are immediately adjacent to district b.
    frontier_a: Vec<usize>,
    /// Blocks in district b that are immediately adjacent to district a.
    frontier_b: Vec<usize>,
}

// ---------------------------------------------------------------------------
// Phase 2 — transfer menus
// ---------------------------------------------------------------------------

/// A connected subgraph of a district that can be atomically transferred to an
/// adjacent district without breaking contiguity of either district.
///
/// Invariants (enforced by `enumerate_transferable_regions`):
///  - `nodes` is non-empty and forms a connected subgraph.
///  - At least one node in `nodes` is adjacent to the destination district.
///  - Removing `nodes` from the source district leaves it contiguous.
///  - Every node in `nodes` is closer to the source/destination boundary than
///    to any other district boundary (no-conflict guarantee).
struct TransferableRegion {
    /// Node indices (block-level unit ids).
    nodes: Vec<usize>,
    /// Sum of `series` over all nodes in the region.
    population: u64,
}

/// Precomputed transfer solutions for one edge, keyed by net_flow.
///
/// For tree edges, each distinct net_flow (one per feasible k in the subtree
/// k-range) has an entry.  For cycle edges, only net_flow = 0 is present
/// initially; non-zero flows are added on demand by Phase 3 when rerouting.
///
/// `solutions[net_flow]` is the result of `solve_transfer_ilp` for that flow:
/// Some(transfers) if a feasible contiguous assignment exists, None otherwise.
/// Positive net_flow = blocks move from a to b; negative = b to a.
///
/// EdgeMenus are stored in a `HashMap<usize, EdgeMenu>` keyed by edge index
/// (into `EqualizationGraph::edges`), so the edge index is not repeated here.
struct EdgeMenu {
    /// ILP results keyed by net_flow.
    solutions: std::collections::HashMap<i64, Option<Vec<Transfer>>>,
}

/// One atomic block transfer: move all blocks in `nodes` from `from` to `to`.
struct Transfer {
    from: u32,
    to: u32,
    /// Block-level unit indices to reassign.
    nodes: Vec<usize>,
}
