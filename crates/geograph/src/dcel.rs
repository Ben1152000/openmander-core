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

macro_rules! idx {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(pub usize);

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self.0)
            }
        }
    };
}

idx!(VertexId);
idx!(HalfEdgeId);
idx!(FaceId);

/// The unbounded (outer) face — always `FaceId(0)`.
pub const OUTER_FACE: FaceId = FaceId(0);

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

/// A vertex with an arbitrary coordinate payload `C` and one incident
/// half-edge (any half-edge whose `origin` is this vertex).
#[derive(Clone, Debug)]
pub struct Vertex<C> {
    pub coords:    C,
    /// Any half-edge leaving this vertex.  `None` for isolated vertices.
    pub half_edge: Option<HalfEdgeId>,
}

/// A directed half-edge.
#[derive(Clone, Debug)]
pub struct HalfEdge {
    /// Vertex this half-edge leaves from.
    pub origin: VertexId,
    /// The other half-edge of the same undirected edge (opposite direction).
    pub twin:   HalfEdgeId,
    /// Next half-edge around `face` in CCW order.
    pub next:   HalfEdgeId,
    /// Previous half-edge around `face` in CCW order.
    pub prev:   HalfEdgeId,
    /// Face to the left of this half-edge.
    pub face:   FaceId,
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
#[derive(Clone, Debug, Default)]
pub struct Dcel<C> {
    pub vertices:   Vec<Vertex<C>>,
    pub half_edges: Vec<HalfEdge>,
    pub faces:      Vec<Face>,
}

impl<C> Dcel<C> {
    /// Create an empty DCEL.  The outer face (`OUTER_FACE`) is pre-inserted.
    pub fn new() -> Self {
        Self {
            vertices:   Vec::new(),
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

    pub fn vertex(&self, id: VertexId)       -> &Vertex<C>  { &self.vertices[id.0] }
    pub fn vertex_mut(&mut self, id: VertexId) -> &mut Vertex<C> { &mut self.vertices[id.0] }

    pub fn half_edge(&self, id: HalfEdgeId)       -> &HalfEdge  { &self.half_edges[id.0] }
    pub fn half_edge_mut(&mut self, id: HalfEdgeId) -> &mut HalfEdge { &mut self.half_edges[id.0] }

    pub fn face(&self, id: FaceId)       -> &Face  { &self.faces[id.0] }
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
    pub fn add_edge(
        &mut self,
        u:          VertexId,
        v:          VertexId,
        face_left:  FaceId,
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
        self.half_edges[he.0].next   = next;
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

    /// Build a simple triangle and verify the half-edge linkage.
    #[test]
    fn triangle() {
        let mut dcel: Dcel<(f64, f64)> = Dcel::new();

        let a = dcel.add_vertex((0.0, 0.0));
        let b = dcel.add_vertex((1.0, 0.0));
        let c = dcel.add_vertex((0.5, 1.0));

        let inner = dcel.add_face(); // the bounded triangle face

        let (ab, ba) = dcel.add_edge(a, b, inner, OUTER_FACE);
        let (bc, cb) = dcel.add_edge(b, c, inner, OUTER_FACE);
        let (ca, ac) = dcel.add_edge(c, a, inner, OUTER_FACE);

        // Inner face cycle: ab → bc → ca → ab
        dcel.set_next(ab, bc);
        dcel.set_next(bc, ca);
        dcel.set_next(ca, ab);

        // Outer face cycle (CW when viewed from outside): ba → ac → cb → ba
        dcel.set_next(ba, ac);
        dcel.set_next(ac, cb);
        dcel.set_next(cb, ba);

        // Verify face cycle length = 3.
        assert_eq!(dcel.face_cycle(ab).count(), 3);
        assert_eq!(dcel.face_cycle(ba).count(), 3);

        // Vertex star of a: ab and ac are the two outgoing half-edges from a.
        let star_a: Vec<_> = dcel.vertex_star(ab).collect();
        assert_eq!(star_a.len(), 2);

        assert_eq!(dcel.num_vertices(), 3);
        assert_eq!(dcel.num_half_edges(), 6);
        assert_eq!(dcel.num_bounded_faces(), 1);
    }
}
