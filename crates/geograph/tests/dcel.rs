use geograph::{Dcel, FaceId, HalfEdgeId, VertexId, OUTER_FACE};

/// A minimal two-triangle planar graph shared by several tests.
///
/// ```
///   c
///  /|\
/// / | \
/// a-+-b
///   d
/// ```
/// Triangles: (a, b, c) and (a, d, b).  Four vertices, four bounded faces
/// (two triangles + outer face component), six undirected edges.
fn two_triangles() -> (Dcel<(f64, f64)>, [VertexId; 4], [FaceId; 2]) {
    let mut dcel: Dcel<(f64, f64)> = Dcel::new();

    let a = dcel.add_vertex((0.0, 0.0));
    let b = dcel.add_vertex((2.0, 0.0));
    let c = dcel.add_vertex((1.0, 2.0));
    let d = dcel.add_vertex((1.0, -2.0));

    let upper = dcel.add_face();
    let lower = dcel.add_face();

    let (ab, ba) = dcel.add_edge(a, b, upper, lower);
    let (bc, cb) = dcel.add_edge(b, c, upper, OUTER_FACE);
    let (ca, ac) = dcel.add_edge(c, a, upper, OUTER_FACE);
    let (bd, db) = dcel.add_edge(b, d, OUTER_FACE, lower);
    let (da, ad) = dcel.add_edge(d, a, OUTER_FACE, lower);

    // Upper face: a→b→c→a
    dcel.set_next(ab, bc);
    dcel.set_next(bc, ca);
    dcel.set_next(ca, ab);

    // Lower face: a→d→b→a  (b→a is the twin of a→b)
    dcel.set_next(ba, ad);
    dcel.set_next(ad, db);
    dcel.set_next(db, ba);

    // Outer face: a→c→b→d→a  (going CW around the outside)
    dcel.set_next(ac, cb);
    dcel.set_next(cb, bd);
    dcel.set_next(bd, da);
    dcel.set_next(da, ac);

    // Point each face at one of its half-edges.
    dcel.face_mut(upper).half_edge = Some(ab);
    dcel.face_mut(lower).half_edge = Some(ba);
    dcel.face_mut(OUTER_FACE).half_edge = Some(ac);

    (dcel, [a, b, c, d], [upper, lower])
}

#[test]
fn counts_are_correct() {
    let (dcel, _, _) = two_triangles();
    assert_eq!(dcel.num_vertices(), 4);
    assert_eq!(dcel.num_half_edges(), 10);
    assert_eq!(dcel.num_bounded_faces(), 2);
}

#[test]
fn face_cycle_lengths() {
    let (dcel, _, faces) = two_triangles();
    let upper_start = dcel.face(faces[0]).half_edge.unwrap();
    let lower_start = dcel.face(faces[1]).half_edge.unwrap();
    assert_eq!(dcel.face_cycle(upper_start).count(), 3);
    assert_eq!(dcel.face_cycle(lower_start).count(), 3);
}

#[test]
fn dest_is_twin_origin() {
    let (dcel, [a, b, ..], _) = two_triangles();
    // The half-edge leaving a should reach b (or some other vertex).
    let he = dcel.vertex(a).half_edge.unwrap();
    let dest = dcel.dest(he);
    assert_ne!(dest, a); // dest is not the same as origin
}

#[test]
fn vertex_star_degree() {
    let (dcel, [a, b, c, d], _) = two_triangles();
    // b is connected to a, c, and d — degree 3
    let start_b = dcel.vertex(b).half_edge.unwrap();
    assert_eq!(dcel.vertex_star(start_b).count(), 3);
    // c is connected to a and b — degree 2
    let start_c = dcel.vertex(c).half_edge.unwrap();
    assert_eq!(dcel.vertex_star(start_c).count(), 2);
}
