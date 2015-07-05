service GraphQueryService {

    list<i64> get_neighbors(1: i64 nodeId),

    list<i64> get_neighbors_atype(1: i64 nodeId, 2: i64 atype),

    list<i64> get_neighbors_attr(1: i64 nodeId, 2: i32 attrId, 3: string attrKey),

    list<i64> get_nodes(1: i32 attrId, 2: string attrKey),

    list<i64> get_nodes2(
        1: i32 attrId1,
        2: string attrKey1,
        3: i32 attrId2,
        4: string attrKey2),

}
