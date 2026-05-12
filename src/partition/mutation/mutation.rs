
// Data structure that stores a generic update to a map
struct Mutation {

}


// Mutations should:
// 1) Keep track of the changes to the district assignments
// 2) Be able to give the updated map score
// 3) Efficiently copy the changes to the base map

// Essentially a mutation should store 
// a) the type and details of the change to the map (maybe block assign?)
// b) the metrics that were affected by the change
// c) a reference to the underlying map
// d) an interface that allows it to act like the changed map without actually making the changes
// e) a way to quickly apply the changes to the base map

// maybe: a chain of mutations on top of each other
