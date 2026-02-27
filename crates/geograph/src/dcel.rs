//! Doubly Connected Edge List (DCEL) — a half-edge data structure for
//! representing planar graphs with full topological and geometric information.
//!
//! # Structure
//!
//! Every undirected edge is represented as a pair of directed **half-edges**
//! (twins).  Each half-edge carries:
//!
//! * `origin`  — the vertex it leaves from
//! * `twin`    — the opposite half-edge (same edge, opposite direction)
//! * `next`    — the next half-edge around the same face (CCW)
//! * `prev`    — the previous half-edge around the same face (CCW)
//! * `face`    — the face to the left of this half-edge
//!
//! Boundary / exterior half-edges point to the unbounded face (`FaceId(0)`).
//!
//! # Indexing
//!
//! All elements are stored in flat `Vec`s and addressed by strongly-typed
//! index wrappers (`VertexId`, `HalfEdgeId`, `FaceId`).  Index `0` is
//! reserved for the unbounded (outer) face; real faces start at `FaceId(1)`.

use std::fmt;

// ---------------------------------------------------------------------------
// Index types
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VertexId(pub usize);

impl fmt::Display for VertexId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VertexId({})", self.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct HalfEdgeId(pub usize);

impl fmt::Display for HalfEdgeId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "HalfEdgeId({})", self.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FaceId(pub usize);

impl fmt::Display for FaceId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FaceId({})", self.0)
    }
}

/// The unbounded (outer) face — always `FaceId(0)`.
pub const OUTER_FACE: FaceId = FaceId(0);

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

/// A vertex with an arbitrary coordinate payload `C` and one incident
/// half-edge (any half-edge whose `origin` is this vertex).
#[derive(Clone, Debug)]
pub struct Vertex<C> {
    pub coords: C,
    /// Any half-edge leaving this vertex.  `None` for isolated vertices.
    pub half_edge: Option<HalfEdgeId>,
}

/// A directed half-edge.
#[derive(Clone, Debug)]
pub struct HalfEdge {
    /// Vertex this half-edge leaves from.
    pub origin: VertexId,
    /// The other half-edge of the same undirected edge (opposite direction).
    pub twin: HalfEdgeId,
    /// Next half-edge around `face` in CCW order.
    pub next: HalfEdgeId,
    /// Previous half-edge around `face` in CCW order.
    pub prev: HalfEdgeId,
    /// Face to the left of this half-edge.
    pub face: FaceId,
}

/// A face (bounded region or the outer face) with one incident half-edge.
#[derive(Clone, Debug)]
pub struct Face {
    /// Any half-edge on the boundary of this face.  `None` for faces with no
    /// boundary (only possible for the outer face of an empty DCEL).
    pub half_edge: Option<HalfEdgeId>,
}

// ---------------------------------------------------------------------------
// DCEL
// ---------------------------------------------------------------------------

/// A Doubly Connected Edge List over vertices with coordinate type `C`.
///
/// `vertices[0]` through `faces[0]` etc. are valid; the outer face is always
/// at index 0 and is pre-inserted on construction.
#[derive(Clone, Debug)]
pub struct Dcel<C> {
    pub vertices:   Vec<Vertex<C>>,
    pub half_edges: Vec<HalfEdge>,
    pub faces:      Vec<Face>,
}

impl<C> Default for Dcel<C> {
    fn default() -> Self { Self::new() }
}

impl<C> Dcel<C> {
    /// Create an empty DCEL. The outer face (`OUTER_FACE`) is pre-inserted.
    pub fn new() -> Self {
        Self {
            vertices: Vec::new(),
            half_edges: Vec::new(),
            // Reserve slot 0 for the outer (unbounded) face.
            faces: vec![Face { half_edge: None }],
        }
    }

    // -----------------------------------------------------------------------
    // Counts
    // -----------------------------------------------------------------------

    pub fn num_vertices(&self)   -> usize { self.vertices.len() }
    pub fn num_half_edges(&self) -> usize { self.half_edges.len() }
    /// Number of faces including the outer face.
    pub fn num_faces(&self)      -> usize { self.faces.len() }
    /// Number of bounded faces (excludes the outer face).
    pub fn num_bounded_faces(&self) -> usize { self.faces.len().saturating_sub(1) }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    pub fn vertex(&self, id: VertexId) -> &Vertex<C> { &self.vertices[id.0] }
    pub fn vertex_mut(&mut self, id: VertexId) -> &mut Vertex<C> { &mut self.vertices[id.0] }

    pub fn half_edge(&self, id: HalfEdgeId) -> &HalfEdge { &self.half_edges[id.0] }
    pub fn half_edge_mut(&mut self, id: HalfEdgeId) -> &mut HalfEdge { &mut self.half_edges[id.0] }

    pub fn face(&self, id: FaceId) -> &Face { &self.faces[id.0] }
    pub fn face_mut(&mut self, id: FaceId) -> &mut Face { &mut self.faces[id.0] }

    // -----------------------------------------------------------------------
    // Builders
    // -----------------------------------------------------------------------

    /// Add an isolated vertex with the given coordinates.
    pub fn add_vertex(&mut self, coords: C) -> VertexId {
        let id = VertexId(self.vertices.len());
        self.vertices.push(Vertex { coords, half_edge: None });
        id
    }

    /// Add a new bounded face (returns its id).
    pub fn add_face(&mut self) -> FaceId {
        let id = FaceId(self.faces.len());
        self.faces.push(Face { half_edge: None });
        id
    }

    /// Add a twin pair of half-edges between `u` and `v`, assigning them to
    /// `face_left` (the face to the left of `u→v`) and `face_right`
    /// (the face to the left of `v→u`).
    ///
    /// `next` and `prev` links are **not** set here; call `set_next` after
    /// building all edges and faces.
    ///
    /// Returns `(uv, vu)` — the half-edge from u to v and its twin.
    pub fn add_edge(&mut self,
        u: VertexId,
        v: VertexId,
        face_left: FaceId,
        face_right: FaceId,
    ) -> (HalfEdgeId, HalfEdgeId) {
        let uv = HalfEdgeId(self.half_edges.len());
        let vu = HalfEdgeId(self.half_edges.len() + 1);

        // Placeholder next/prev; caller must fix up.
        self.half_edges.push(HalfEdge { origin: u, twin: vu, next: uv, prev: uv, face: face_left  });
        self.half_edges.push(HalfEdge { origin: v, twin: uv, next: vu, prev: vu, face: face_right });

        // Point vertices at these half-edges if they have none yet.
        if self.vertices[u.0].half_edge.is_none() { self.vertices[u.0].half_edge = Some(uv); }
        if self.vertices[v.0].half_edge.is_none() { self.vertices[v.0].half_edge = Some(vu); }

        (uv, vu)
    }

    /// Set `he.next = next` and `next.prev = he`.
    pub fn set_next(&mut self, he: HalfEdgeId, next: HalfEdgeId) {
        self.half_edges[he.0].next = next;
        self.half_edges[next.0].prev = he;
    }

    // -----------------------------------------------------------------------
    // Traversal iterators
    // -----------------------------------------------------------------------

    /// Iterate over all half-edges around the face of `start` in CCW order,
    /// starting (and ending just before returning to) `start`.
    pub fn face_cycle(&self, start: HalfEdgeId) -> FaceCycle<'_, C> {
        FaceCycle { dcel: self, start, current: start, done: false }
    }

    /// Iterate over all outgoing half-edges around a vertex in CCW order
    /// (using `twin.next` links), starting from `start`.
    pub fn vertex_star(&self, start: HalfEdgeId) -> VertexStar<'_, C> {
        VertexStar { dcel: self, start, current: start, done: false }
    }

    // -----------------------------------------------------------------------
    // Destination helper
    // -----------------------------------------------------------------------

    /// The vertex at the head (destination) of a half-edge.
    pub fn dest(&self, he: HalfEdgeId) -> VertexId {
        self.half_edges[self.half_edges[he.0].twin.0].origin
    }
}

// ---------------------------------------------------------------------------
// Iterators
// ---------------------------------------------------------------------------

/// Iterator over half-edges in a face cycle (CCW).
pub struct FaceCycle<'a, C> {
    dcel:    &'a Dcel<C>,
    start:   HalfEdgeId,
    current: HalfEdgeId,
    done:    bool,
}

impl<'a, C> Iterator for FaceCycle<'a, C> {
    type Item = HalfEdgeId;

    fn next(&mut self) -> Option<HalfEdgeId> {
        if self.done { return None; }
        let he = self.current;
        self.current = self.dcel.half_edges[he.0].next;
        if self.current == self.start { self.done = true; }
        Some(he)
    }
}

/// Iterator over half-edges in a vertex star (CCW), using `twin.next`.
pub struct VertexStar<'a, C> {
    dcel:    &'a Dcel<C>,
    start:   HalfEdgeId,
    current: HalfEdgeId,
    done:    bool,
}

impl<'a, C> Iterator for VertexStar<'a, C> {
    type Item = HalfEdgeId;

    fn next(&mut self) -> Option<HalfEdgeId> {
        if self.done { return None; }
        let he = self.current;
        let twin = self.dcel.half_edges[he.0].twin;
        self.current = self.dcel.half_edges[twin.0].next;
        if self.current == self.start { self.done = true; }
        Some(he)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Fixtures
    // -----------------------------------------------------------------------

    /// A single bounded triangle face plus the outer face.
    ///
    /// ```text
    ///     c
    ///    / \
    ///   /   \
    ///  a-----b
    /// ```
    ///
    /// Inner cycle (CCW): ab → bc → ca
    /// Outer cycle (CW):  ba → ac → cb
    fn make_triangle() -> (Dcel<(f64, f64)>, [VertexId; 3], FaceId, [HalfEdgeId; 6]) {
        let mut d = Dcel::new();
        let a = d.add_vertex((0.0, 0.0));
        let b = d.add_vertex((1.0, 0.0));
        let c = d.add_vertex((0.5, 1.0));
        let inner = d.add_face();
        let (ab, ba) = d.add_edge(a, b, inner, OUTER_FACE);
        let (bc, cb) = d.add_edge(b, c, inner, OUTER_FACE);
        let (ca, ac) = d.add_edge(c, a, inner, OUTER_FACE);
        d.set_next(ab, bc); d.set_next(bc, ca); d.set_next(ca, ab);
        d.set_next(ba, ac); d.set_next(ac, cb); d.set_next(cb, ba);
        d.face_mut(inner).half_edge      = Some(ab);
        d.face_mut(OUTER_FACE).half_edge = Some(ba);
        (d, [a, b, c], inner, [ab, ba, bc, cb, ca, ac])
    }

    /// A single bounded square face plus the outer face.
    ///
    /// ```text
    ///  a---b
    ///  |   |
    ///  d---c
    /// ```
    ///
    /// Inner cycle (CCW): ab → bc → cd → da
    /// Outer cycle (CW):  ba → ad → dc → cb
    fn make_square() -> (Dcel<(f64, f64)>, [VertexId; 4], FaceId, [HalfEdgeId; 8]) {
        let mut d = Dcel::new();
        let a = d.add_vertex((0.0, 1.0));
        let b = d.add_vertex((1.0, 1.0));
        let c = d.add_vertex((1.0, 0.0));
        let e = d.add_vertex((0.0, 0.0)); // 'd' is a keyword in some contexts
        let inner = d.add_face();
        let (ab, ba) = d.add_edge(a, b, inner, OUTER_FACE);
        let (bc, cb) = d.add_edge(b, c, inner, OUTER_FACE);
        let (cd, dc) = d.add_edge(c, e, inner, OUTER_FACE);
        let (da, ad) = d.add_edge(e, a, inner, OUTER_FACE);
        d.set_next(ab, bc); d.set_next(bc, cd); d.set_next(cd, da); d.set_next(da, ab);
        d.set_next(ba, ad); d.set_next(ad, dc); d.set_next(dc, cb); d.set_next(cb, ba);
        d.face_mut(inner).half_edge      = Some(ab);
        d.face_mut(OUTER_FACE).half_edge = Some(ba);
        (d, [a, b, c, e], inner, [ab, ba, bc, cb, cd, dc, da, ad])
    }

    /// A wheel graph: one hub vertex `o` connected to four rim vertices
    /// `n`, `w`, `s`, `e` (N/W/S/E), forming four triangular inner faces
    /// plus one outer (square) face.
    ///
    /// ```text
    ///     n
    ///    /|\
    ///   / | \
    ///  w--o--e
    ///   \ | /
    ///    \|/
    ///     s
    /// ```
    ///
    /// CCW spoke order around o: e(0°), n(90°), w(180°), s(270°)
    /// Inner faces (CCW triangles):
    ///   f1 = o→e→n (NE), f2 = o→n→w (NW), f3 = o→w→s (SW), f4 = o→s→e (SE)
    /// Outer face: ne → es → sw → wn (quad, CCW from outside)
    fn make_wheel() -> (Dcel<(f64, f64)>, [VertexId; 5], [FaceId; 4], [HalfEdgeId; 16]) {
        let mut d = Dcel::new();
        let o = d.add_vertex(( 0.0,  0.0)); // center
        let n = d.add_vertex(( 0.0,  1.0)); // north
        let w = d.add_vertex((-1.0,  0.0)); // west
        let s = d.add_vertex(( 0.0, -1.0)); // south
        let e = d.add_vertex(( 1.0,  0.0)); // east

        let f1 = d.add_face(); // NE: o-e-n
        let f2 = d.add_face(); // NW: o-n-w
        let f3 = d.add_face(); // SW: o-w-s
        let f4 = d.add_face(); // SE: o-s-e

        // Spoke edges (face_left, face_right):
        //   o→e is on the f1 (NE) side; e→o is on the f4 (SE) side
        let (oe, eo) = d.add_edge(o, e, f1, f4);
        let (on, no) = d.add_edge(o, n, f2, f1);
        let (ow, wo) = d.add_edge(o, w, f3, f2);
        let (os, so) = d.add_edge(o, s, f4, f3);

        // Rim edges
        let (en, ne) = d.add_edge(e, n, f1, OUTER_FACE);
        let (nw, wn) = d.add_edge(n, w, f2, OUTER_FACE);
        let (ws, sw) = d.add_edge(w, s, f3, OUTER_FACE);
        let (se, es) = d.add_edge(s, e, f4, OUTER_FACE);

        // f1 (NE, o→e→n→o):
        d.set_next(oe, en); d.set_next(en, no); d.set_next(no, oe);
        // f2 (NW, o→n→w→o):
        d.set_next(on, nw); d.set_next(nw, wo); d.set_next(wo, on);
        // f3 (SW, o→w→s→o):
        d.set_next(ow, ws); d.set_next(ws, so); d.set_next(so, ow);
        // f4 (SE, o→s→e→o):
        d.set_next(os, se); d.set_next(se, eo); d.set_next(eo, os);
        // Outer face (ne→es→sw→wn):
        d.set_next(ne, es); d.set_next(es, sw); d.set_next(sw, wn); d.set_next(wn, ne);

        d.face_mut(f1).half_edge = Some(oe);
        d.face_mut(f2).half_edge = Some(on);
        d.face_mut(f3).half_edge = Some(ow);
        d.face_mut(f4).half_edge = Some(os);
        d.face_mut(OUTER_FACE).half_edge = Some(ne);

        (d, [o, n, w, s, e], [f1, f2, f3, f4],
         [oe, eo, on, no, ow, wo, os, so, en, ne, nw, wn, ws, sw, se, es])
    }

    /// An outer triangle (a-b-c) containing a smaller inner triangle (p-q-r),
    /// with no bridge edge.  The annular region between them is face C, which
    /// has **two disjoint boundary cycles**: the outer boundary (ab→bc→ca) and
    /// the hole boundary (rq→qp→pr).
    ///
    /// ```text
    ///       a
    ///      / \
    ///     /   \
    ///    / p-r \
    ///   / / B \ \
    ///  / q     \ \
    /// b-----------c
    /// ```
    ///
    /// Faces:
    ///   B (inner_b) = interior of p-q-r
    ///   C (annular) = annular region between the two triangles
    ///   OUTER_FACE  = exterior of a-b-c
    ///
    /// C's outer boundary (CCW): ab → bc → ca
    /// C's hole  boundary (CW around hole): rq → qp → pr
    fn make_nested() -> (Dcel<(f64, f64)>, [VertexId; 6], [FaceId; 2], [HalfEdgeId; 12]) {
        let mut d = Dcel::new();
        // Outer triangle (CCW)
        let a = d.add_vertex(( 0.0,  2.0));
        let b = d.add_vertex((-2.0, -1.0));
        let c = d.add_vertex(( 2.0, -1.0));
        // Inner triangle (CCW)
        let p = d.add_vertex(( 0.0,  0.5));
        let q = d.add_vertex((-0.5, -0.3));
        let r = d.add_vertex(( 0.5, -0.3));

        let inner_b = d.add_face(); // B: inside p-q-r
        let annular = d.add_face(); // C: annular region

        // Outer triangle: annular (C) on the inside, OUTER_FACE on the outside
        let (ab, ba) = d.add_edge(a, b, annular, OUTER_FACE);
        let (bc, cb) = d.add_edge(b, c, annular, OUTER_FACE);
        let (ca, ac) = d.add_edge(c, a, annular, OUTER_FACE);
        // Inner triangle: B on the inside, annular (C) on the outside
        let (pq, qp) = d.add_edge(p, q, inner_b, annular);
        let (qr, rq) = d.add_edge(q, r, inner_b, annular);
        let (rp, pr) = d.add_edge(r, p, inner_b, annular);

        // Outer boundary of C (CCW: a→b→c→a):
        d.set_next(ab, bc); d.set_next(bc, ca); d.set_next(ca, ab);
        // Hole boundary of C (CW around hole: r→q→p→r):
        d.set_next(rq, qp); d.set_next(qp, pr); d.set_next(pr, rq);
        // Inner face B (CCW: p→q→r→p):
        d.set_next(pq, qr); d.set_next(qr, rp); d.set_next(rp, pq);
        // Outer face (CCW from outside = CW around outer triangle: a→c→b→a):
        d.set_next(ac, cb); d.set_next(cb, ba); d.set_next(ba, ac);

        d.face_mut(inner_b).half_edge = Some(pq);
        d.face_mut(annular).half_edge = Some(ab); // outer boundary only
        d.face_mut(OUTER_FACE).half_edge = Some(ac);

        (d, [a, b, c, p, q, r], [inner_b, annular], [ab, ba, bc, cb, ca, ac, pq, qp, qr, rq, rp, pr])
    }

    // -----------------------------------------------------------------------
    // Initial state
    // -----------------------------------------------------------------------

    #[test]
    fn new_has_no_vertices() {
        let d = Dcel::<(f64, f64)>::new();
        assert_eq!(d.num_vertices(), 0);
    }

    #[test]
    fn new_has_no_half_edges() {
        let d = Dcel::<(f64, f64)>::new();
        assert_eq!(d.num_half_edges(), 0);
    }

    #[test]
    fn new_has_exactly_one_face() {
        let d = Dcel::<(f64, f64)>::new();
        assert_eq!(d.num_faces(), 1);
    }

    #[test]
    fn new_has_zero_bounded_faces() {
        let d = Dcel::<(f64, f64)>::new();
        assert_eq!(d.num_bounded_faces(), 0);
    }

    #[test]
    fn outer_face_is_face_id_zero() {
        assert_eq!(OUTER_FACE, FaceId(0));
    }

    #[test]
    fn outer_face_half_edge_is_none_initially() {
        let d = Dcel::<(f64, f64)>::new();
        assert!(d.face(OUTER_FACE).half_edge.is_none());
    }

    #[test]
    fn default_equals_new() {
        let d1 = Dcel::<(f64, f64)>::new();
        let d2 = Dcel::<(f64, f64)>::default();
        assert_eq!(d1.num_vertices(),   d2.num_vertices());
        assert_eq!(d1.num_half_edges(), d2.num_half_edges());
        assert_eq!(d1.num_faces(),      d2.num_faces());
    }

    // -----------------------------------------------------------------------
    // add_vertex
    // -----------------------------------------------------------------------

    #[test]
    fn add_vertex_returns_sequential_ids() {
        let mut d = Dcel::new();
        let v0 = d.add_vertex((0.0, 0.0));
        let v1 = d.add_vertex((1.0, 0.0));
        let v2 = d.add_vertex((2.0, 0.0));
        assert_eq!(v0, VertexId(0));
        assert_eq!(v1, VertexId(1));
        assert_eq!(v2, VertexId(2));
    }

    #[test]
    fn add_vertex_stores_coordinates() {
        let mut d = Dcel::new();
        let v = d.add_vertex((3.5, -1.0));
        assert_eq!(d.vertex(v).coords, (3.5, -1.0));
    }

    #[test]
    fn add_vertex_half_edge_is_none() {
        let mut d = Dcel::new();
        let v = d.add_vertex((0.0, 0.0));
        assert!(d.vertex(v).half_edge.is_none());
    }

    #[test]
    fn add_vertex_increments_count() {
        let mut d = Dcel::new();
        for i in 0..5 {
            assert_eq!(d.num_vertices(), i);
            d.add_vertex((i as f64, 0.0));
        }
        assert_eq!(d.num_vertices(), 5);
    }

    // -----------------------------------------------------------------------
    // add_face
    // -----------------------------------------------------------------------

    #[test]
    fn add_face_first_id_is_one() {
        // FaceId(0) is reserved for OUTER_FACE.
        let mut d = Dcel::<(f64, f64)>::new();
        let f = d.add_face();
        assert_eq!(f, FaceId(1));
    }

    #[test]
    fn add_face_returns_sequential_ids() {
        let mut d = Dcel::<(f64, f64)>::new();
        let f1 = d.add_face();
        let f2 = d.add_face();
        let f3 = d.add_face();
        assert_eq!(f1, FaceId(1));
        assert_eq!(f2, FaceId(2));
        assert_eq!(f3, FaceId(3));
    }

    #[test]
    fn add_face_half_edge_is_none() {
        let mut d = Dcel::<(f64, f64)>::new();
        let f = d.add_face();
        assert!(d.face(f).half_edge.is_none());
    }

    #[test]
    fn add_face_increments_bounded_count() {
        let mut d = Dcel::<(f64, f64)>::new();
        assert_eq!(d.num_bounded_faces(), 0);
        d.add_face();
        assert_eq!(d.num_bounded_faces(), 1);
        d.add_face();
        assert_eq!(d.num_bounded_faces(), 2);
    }

    // -----------------------------------------------------------------------
    // add_edge
    // -----------------------------------------------------------------------

    #[test]
    fn add_edge_returns_consecutive_ids() {
        let mut d = Dcel::new();
        let u = d.add_vertex((0.0, 0.0));
        let v = d.add_vertex((1.0, 0.0));
        let (uv, vu) = d.add_edge(u, v, OUTER_FACE, OUTER_FACE);
        assert_eq!(uv, HalfEdgeId(0));
        assert_eq!(vu, HalfEdgeId(1));
    }

    #[test]
    fn add_edge_twin_ids_differ_by_one() {
        let mut d = Dcel::new();
        let u = d.add_vertex((0.0, 0.0));
        let v = d.add_vertex((1.0, 0.0));
        let (uv, vu) = d.add_edge(u, v, OUTER_FACE, OUTER_FACE);
        assert_eq!(uv.0 + 1, vu.0);
    }

    #[test]
    fn add_edge_twins_reference_each_other() {
        let mut d = Dcel::new();
        let u = d.add_vertex((0.0, 0.0));
        let v = d.add_vertex((1.0, 0.0));
        let (uv, vu) = d.add_edge(u, v, OUTER_FACE, OUTER_FACE);
        assert_eq!(d.half_edge(uv).twin, vu);
        assert_eq!(d.half_edge(vu).twin, uv);
    }

    #[test]
    fn add_edge_origins_are_correct() {
        let mut d = Dcel::new();
        let u = d.add_vertex((0.0, 0.0));
        let v = d.add_vertex((1.0, 0.0));
        let (uv, vu) = d.add_edge(u, v, OUTER_FACE, OUTER_FACE);
        assert_eq!(d.half_edge(uv).origin, u);
        assert_eq!(d.half_edge(vu).origin, v);
    }

    #[test]
    fn add_edge_faces_are_correct() {
        let mut d = Dcel::new();
        let u = d.add_vertex((0.0, 0.0));
        let v = d.add_vertex((1.0, 0.0));
        let f = d.add_face();
        let (uv, vu) = d.add_edge(u, v, f, OUTER_FACE);
        assert_eq!(d.half_edge(uv).face, f);
        assert_eq!(d.half_edge(vu).face, OUTER_FACE);
    }

    #[test]
    fn add_edge_initial_next_and_prev_are_self() {
        // Before set_next is called, next/prev are placeholder self-references.
        let mut d = Dcel::new();
        let u = d.add_vertex((0.0, 0.0));
        let v = d.add_vertex((1.0, 0.0));
        let (uv, vu) = d.add_edge(u, v, OUTER_FACE, OUTER_FACE);
        assert_eq!(d.half_edge(uv).next, uv);
        assert_eq!(d.half_edge(uv).prev, uv);
        assert_eq!(d.half_edge(vu).next, vu);
        assert_eq!(d.half_edge(vu).prev, vu);
    }

    #[test]
    fn add_edge_sets_vertex_half_edge_on_first_use() {
        let mut d = Dcel::new();
        let u = d.add_vertex((0.0, 0.0));
        let v = d.add_vertex((1.0, 0.0));
        let (uv, vu) = d.add_edge(u, v, OUTER_FACE, OUTER_FACE);
        assert_eq!(d.vertex(u).half_edge, Some(uv));
        assert_eq!(d.vertex(v).half_edge, Some(vu));
    }

    #[test]
    fn add_edge_does_not_overwrite_existing_vertex_half_edge() {
        let mut d = Dcel::new();
        let u = d.add_vertex((0.0, 0.0));
        let v = d.add_vertex((1.0, 0.0));
        let w = d.add_vertex((2.0, 0.0));
        let (uv, _) = d.add_edge(u, v, OUTER_FACE, OUTER_FACE);
        let (uw, _) = d.add_edge(u, w, OUTER_FACE, OUTER_FACE);
        // u's half_edge should still point to the first edge added from u.
        assert_eq!(d.vertex(u).half_edge, Some(uv));
        assert_ne!(d.vertex(u).half_edge, Some(uw));
    }

    #[test]
    fn add_edge_increments_half_edge_count_by_two() {
        let mut d = Dcel::new();
        let u = d.add_vertex((0.0, 0.0));
        let v = d.add_vertex((1.0, 0.0));
        assert_eq!(d.num_half_edges(), 0);
        d.add_edge(u, v, OUTER_FACE, OUTER_FACE);
        assert_eq!(d.num_half_edges(), 2);
        let w = d.add_vertex((2.0, 0.0));
        d.add_edge(v, w, OUTER_FACE, OUTER_FACE);
        assert_eq!(d.num_half_edges(), 4);
    }

    // -----------------------------------------------------------------------
    // set_next
    // -----------------------------------------------------------------------

    #[test]
    fn set_next_sets_next_link() {
        let mut d = Dcel::new();
        let u = d.add_vertex((0.0, 0.0));
        let v = d.add_vertex((1.0, 0.0));
        let w = d.add_vertex((2.0, 0.0));
        let (uv, _) = d.add_edge(u, v, OUTER_FACE, OUTER_FACE);
        let (vw, _) = d.add_edge(v, w, OUTER_FACE, OUTER_FACE);
        d.set_next(uv, vw);
        assert_eq!(d.half_edge(uv).next, vw);
    }

    #[test]
    fn set_next_sets_prev_link() {
        let mut d = Dcel::new();
        let u = d.add_vertex((0.0, 0.0));
        let v = d.add_vertex((1.0, 0.0));
        let w = d.add_vertex((2.0, 0.0));
        let (uv, _) = d.add_edge(u, v, OUTER_FACE, OUTER_FACE);
        let (vw, _) = d.add_edge(v, w, OUTER_FACE, OUTER_FACE);
        d.set_next(uv, vw);
        assert_eq!(d.half_edge(vw).prev, uv);
    }

    #[test]
    fn set_next_is_independent_for_each_half_edge() {
        let (d, _, _, [ab, ba, bc, cb, ca, ac]) = make_triangle();
        assert_eq!(d.half_edge(ab).next, bc);
        assert_eq!(d.half_edge(bc).next, ca);
        assert_eq!(d.half_edge(ca).next, ab);
        assert_eq!(d.half_edge(ba).next, ac);
        assert_eq!(d.half_edge(ac).next, cb);
        assert_eq!(d.half_edge(cb).next, ba);
    }

    // -----------------------------------------------------------------------
    // face_cycle
    // -----------------------------------------------------------------------

    #[test]
    fn face_cycle_triangle_inner_has_length_three() {
        let (d, _, _, [ab, ..]) = make_triangle();
        assert_eq!(d.face_cycle(ab).count(), 3);
    }

    #[test]
    fn face_cycle_triangle_outer_has_length_three() {
        let (d, _, _, [_, ba, ..]) = make_triangle();
        assert_eq!(d.face_cycle(ba).count(), 3);
    }

    #[test]
    fn face_cycle_square_has_length_four() {
        let (d, _, _, [ab, ba, ..]) = make_square();
        assert_eq!(d.face_cycle(ab).count(), 4); // inner
        assert_eq!(d.face_cycle(ba).count(), 4); // outer
    }

    #[test]
    fn face_cycle_starts_with_given_half_edge() {
        let (d, _, _, [ab, ..]) = make_triangle();
        let first = d.face_cycle(ab).next().unwrap();
        assert_eq!(first, ab);
    }

    #[test]
    fn face_cycle_visits_correct_faces() {
        let (d, _, inner, [ab, ba, _, _, _, _]) = make_triangle();
        // Every half-edge in the inner cycle must belong to `inner`.
        for he in d.face_cycle(ab) {
            assert_eq!(d.half_edge(he).face, inner);
        }
        // Every half-edge in the outer cycle must belong to OUTER_FACE.
        for he in d.face_cycle(ba) {
            assert_eq!(d.half_edge(he).face, OUTER_FACE);
        }
    }

    #[test]
    fn face_cycle_all_half_edges_collected() {
        let (d, _, _, [ab, ba, bc, cb, ca, ac]) = make_triangle();
        let inner_cycle: Vec<_> = d.face_cycle(ab).collect();
        assert!(inner_cycle.contains(&ab));
        assert!(inner_cycle.contains(&bc));
        assert!(inner_cycle.contains(&ca));
        let outer_cycle: Vec<_> = d.face_cycle(ba).collect();
        assert!(outer_cycle.contains(&ba));
        assert!(outer_cycle.contains(&ac));
        assert!(outer_cycle.contains(&cb));
    }

    #[test]
    fn face_cycle_same_result_from_any_starting_half_edge() {
        let (d, _, _, [ab, _, bc, _, ca, _]) = make_triangle();
        let from_ab: Vec<_> = d.face_cycle(ab).collect();
        let from_bc: Vec<_> = d.face_cycle(bc).collect();
        let from_ca: Vec<_> = d.face_cycle(ca).collect();
        // All three contain the same set of half-edges.
        let mut s_ab = from_ab.clone(); s_ab.sort();
        let mut s_bc = from_bc.clone(); s_bc.sort();
        let mut s_ca = from_ca.clone(); s_ca.sort();
        assert_eq!(s_ab, s_bc);
        assert_eq!(s_bc, s_ca);
    }

    // -----------------------------------------------------------------------
    // vertex_star
    // -----------------------------------------------------------------------

    #[test]
    fn vertex_star_degree_two_in_triangle() {
        let (d, [_, _, _], _, [ab, ..]) = make_triangle();
        // Every vertex in a triangle touches exactly 2 edges.
        assert_eq!(d.vertex_star(ab).count(), 2); // star of a
    }

    #[test]
    fn vertex_star_degree_two_in_square() {
        let (d, _, _, [ab, ..]) = make_square();
        // Every vertex in a square touches exactly 2 edges.
        assert_eq!(d.vertex_star(ab).count(), 2);
    }

    #[test]
    fn vertex_star_all_outgoing_from_same_vertex() {
        let (d, [a, ..], _, [ab, ..]) = make_triangle();
        for he in d.vertex_star(ab) {
            assert_eq!(d.half_edge(he).origin, a);
        }
    }

    #[test]
    fn vertex_star_no_duplicates() {
        let (d, _, _, [ab, ..]) = make_square();
        let star: Vec<_> = d.vertex_star(ab).collect();
        let mut deduped = star.clone();
        deduped.sort();
        deduped.dedup();
        assert_eq!(star.len(), deduped.len());
    }

    // -----------------------------------------------------------------------
    // dest
    // -----------------------------------------------------------------------

    #[test]
    fn dest_is_twin_origin() {
        let (d, [a, b, c], _, [ab, ba, bc, cb, ca, ac]) = make_triangle();
        assert_eq!(d.dest(ab), b);
        assert_eq!(d.dest(ba), a);
        assert_eq!(d.dest(bc), c);
        assert_eq!(d.dest(cb), b);
        assert_eq!(d.dest(ca), a);
        assert_eq!(d.dest(ac), c);
    }

    // -----------------------------------------------------------------------
    // Index type properties
    // -----------------------------------------------------------------------

    #[test]
    fn index_types_display_correctly() {
        assert_eq!(VertexId(7).to_string(),   "VertexId(7)");
        assert_eq!(HalfEdgeId(3).to_string(), "HalfEdgeId(3)");
        assert_eq!(FaceId(0).to_string(),     "FaceId(0)");
    }

    #[test]
    fn index_types_are_ordered() {
        assert!(VertexId(0) < VertexId(1));
        assert!(HalfEdgeId(4) < HalfEdgeId(5));
        assert!(FaceId(0) < FaceId(1));
    }

    #[test]
    fn index_types_are_equal_by_value() {
        assert_eq!(VertexId(3), VertexId(3));
        assert_ne!(VertexId(3), VertexId(4));
    }

    // -----------------------------------------------------------------------
    // wheel: hub vertex with degree 4
    // -----------------------------------------------------------------------

    #[test]
    fn wheel_has_correct_counts() {
        let (d, _, _, _) = make_wheel();
        assert_eq!(d.num_vertices(), 5);
        assert_eq!(d.num_half_edges(), 16);
        assert_eq!(d.num_bounded_faces(), 4);
    }

    #[test]
    fn hub_vertex_has_degree_four() {
        let (d, _, _, [oe, ..]) = make_wheel();
        assert_eq!(d.vertex_star(oe).count(), 4);
    }

    #[test]
    fn hub_all_spokes_originate_at_hub() {
        let (d, [o, ..], _, [oe, ..]) = make_wheel();
        for he in d.vertex_star(oe) {
            assert_eq!(d.half_edge(he).origin, o);
        }
    }

    #[test]
    fn rim_vertices_have_degree_three() {
        let (d, [_, n, w, s, e], _, _) = make_wheel();
        for v in [n, w, s, e] {
            let start = d.vertex(v).half_edge.unwrap();
            assert_eq!(d.vertex_star(start).count(), 3);
        }
    }

    #[test]
    fn all_inner_faces_are_triangles() {
        let (d, _, faces, _) = make_wheel();
        for f in faces {
            let start = d.face(f).half_edge.unwrap();
            assert_eq!(d.face_cycle(start).count(), 3);
        }
    }

    #[test]
    fn wheel_outer_face_is_quadrilateral() {
        let (d, _, _, _) = make_wheel();
        let start = d.face(OUTER_FACE).half_edge.unwrap();
        assert_eq!(d.face_cycle(start).count(), 4);
    }

    // -----------------------------------------------------------------------
    // nested: one face completely surrounded by another
    // -----------------------------------------------------------------------

    #[test]
    fn nested_has_correct_counts() {
        let (d, _, _, _) = make_nested();
        assert_eq!(d.num_vertices(), 6);
        assert_eq!(d.num_half_edges(), 12);
        assert_eq!(d.num_bounded_faces(), 2);
    }

    #[test]
    fn nested_inner_face_cycle_has_length_three() {
        let (d, _, [inner_b, _], _) = make_nested();
        let start = d.face(inner_b).half_edge.unwrap();
        assert_eq!(d.face_cycle(start).count(), 3);
    }

    #[test]
    fn nested_outer_face_cycle_has_length_three() {
        let (d, _, _, [_, _, _, _, _, ac, ..]) = make_nested();
        assert_eq!(d.face_cycle(ac).count(), 3);
    }

    /// Face C (annular) has two boundary cycles that are disjoint and
    /// unreachable from each other via `next` links.
    #[test]
    fn nested_annular_face_has_two_disjoint_boundary_cycles() {
        // ab = outer boundary start; rq = hole boundary start
        let (d, _, [_, annular], [ab, _, _, _, _, _, _, _, _, rq, ..]) = make_nested();

        let outer_cycle: Vec<_> = d.face_cycle(ab).collect();
        let hole_cycle:  Vec<_> = d.face_cycle(rq).collect();

        assert_eq!(outer_cycle.len(), 3);
        assert_eq!(hole_cycle.len(), 3);

        // The two cycles share no half-edges.
        for he in &hole_cycle {
            assert!(!outer_cycle.contains(he));
        }

        // Both cycles belong to the same annular face.
        for &he in outer_cycle.iter().chain(&hole_cycle) {
            assert_eq!(d.half_edge(he).face, annular);
        }
    }

    /// The face pointer of the annular face leads only to the outer boundary;
    /// the hole cycle is not reachable from it.
    #[test]
    fn nested_annular_face_pointer_reaches_only_outer_boundary() {
        let (d, _, [_, annular], _) = make_nested();
        let start = d.face(annular).half_edge.unwrap();
        assert_eq!(d.face_cycle(start).count(), 3);
    }

    #[test]
    fn nested_all_vertices_have_degree_two() {
        let (d, vertices, _, _) = make_nested();
        for v in vertices {
            let start = d.vertex(v).half_edge.unwrap();
            assert_eq!(d.vertex_star(start).count(), 2);
        }
    }
}
