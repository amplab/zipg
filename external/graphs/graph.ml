open Util
open Printf


type ('a, 'b) node = {
	id: int;
	edges: (int, 'b) Hashtbl.t;
	label: 'a;
	}

(** The graph data type. 'a and 'b are the node and edge label types, respectively. *)
type ('a, 'b) t = {
	(* Hashtable mapping node id to node data structure *)
	nodes: (int, (('a, 'b) node)) Hashtbl.t;
	
	(* Hashtable used to do lookups mapping node labels to associated node ids. *)
	labels: ('a, (int, unit) Hashtbl.t) Hashtbl.t;

	mutable lowest_unused_id: int;
	}
	
(** Creates a new empty graph. *)
let create () =
	{
	nodes = Hashtbl.create 1;
	labels = Hashtbl.create 1;
	lowest_unused_id = 0;
	}

let find_node g i =
	Hashtbl.find g.nodes i

let find_nodes_with_label g label =
	Hashtbl.find g.labels label

let find_id g l =
	let node_set = Hashtbl.find g.labels l in
	if Hashtbl.length node_set != 1 then
		raise (Failure "find_id: multiple nodes with this label");
	List.hd (hashtbl_keys node_set)

(* Returns the label of edge i -> j *)
let find_edge g i j =
	Hashtbl.find (find_node g i).edges j

let contains_node g i =
	Hashtbl.mem g.nodes i

let contains_label g l =
	Hashtbl.mem g.labels l

let num_nodes g =
	Hashtbl.length g.nodes

let compute_num_directed_edges g =
	let sum = ref 0 in
	foreach_hashtbl g.nodes (fun _ v ->
		sum := !sum + (Hashtbl.length v.edges)
		);
	!sum

(* Typically you should not use this. Use `add_node' instead.
 * But it's OK to use this if you need to. *)
let add_node_with_id g id label =
	let v = {
		id = id;
		edges = Hashtbl.create 1;
		label = label;
		}
		in
	Hashtbl.add g.nodes v.id v;
	g.lowest_unused_id <- max g.lowest_unused_id (id+1);

	(* Label stuff *)
	if not (Hashtbl.mem g.labels label) then
		Hashtbl.add g.labels label (Hashtbl.create 1);
	let lbl_node_set = Hashtbl.find g.labels label in
	Hashtbl.add lbl_node_set v.id ();

	v

(* Take a peek at what the ID of the next created node will be *)
let next_new_node_id g =
	g.lowest_unused_id

(* Returns the new node. *)
let add_node g label =
	add_node_with_id g g.lowest_unused_id label

let add_directed_edge g i j label =
	let v = find_node g i in
	Hashtbl.add v.edges j label

let add_edge g i j label =
	add_directed_edge g i j label;
	add_directed_edge g j i label

let remove_directed_edge g i j =
	let v = find_node g i in
	Hashtbl.remove v.edges j

let remove_edge g i j =
	remove_directed_edge g i j;
	remove_directed_edge g j i

(* Warning: Only use this on undirected graphs!
 * For directed graphs, it may leave edges dangling back to a nonextant node.
 * To do this right, we'd have to have backpointers so each node knows about
 * its in-edges and out-edges.
 *)
let remove_node g i =
	let v = find_node g i in
	foreach_hashtbl v.edges (fun j _ ->
		remove_directed_edge g j i);
	let l_hash = Hashtbl.find g.labels v.label in
	Hashtbl.remove l_hash i;
	if Hashtbl.length l_hash = 0 then
		Hashtbl.remove g.labels v.label;
	Hashtbl.remove g.nodes i

(* Write this graph in neato (graphviz) format to the given outchannel *)
let write_neato g node2neato edge2neato outchannel =
	fprintf outchannel "graph foo {\n";
	fprintf outchannel "overlap=no;\nnode [fontsize=18];\n";
	foreach_hashtbl g.nodes (fun i v ->
		fprintf outchannel "n%d [%s];\n" i (node2neato v);
		foreach_hashtbl v.edges (fun j edgelabel ->
			if i <= j then
				fprintf outchannel "n%d -- n%d [%s]\n" i j (edge2neato edgelabel)
			)
		);
	fprintf outchannel "}\n"

(* Given two functions that map node labels to some new labels,
 * and edge labels to some new edge labels, 
 * construct a new graph with those new labels.
 *)
let map g node_map_fn edge_map_fn =
	let g2 = create () in
	(* Add all the nodes first *)
	foreach_hashtbl g.nodes (fun i v ->
		ignore (add_node_with_id g2 i (node_map_fn v.label))
		);
	(* Now add edges *)
	foreach_hashtbl g.nodes (fun i v ->
		foreach_hashtbl v.edges (fun j label ->
			add_directed_edge g2 i j (edge_map_fn label)
			)
		);
	g2
	

(* Write this graph in simple text format, listing the edges in the form
 * i j e   where i and j are node IDs and e = edge2string called on the edge label.
 *)
let write g edge2string outchannel =
	foreach_hashtbl g.nodes (fun i v ->
		foreach_hashtbl v.edges (fun j edge_label ->
			fprintf outchannel "%d %d %s\n" i j (edge2string edge_label)
			)
		)

(* Read a graph from simple text format, listing the edges in the form
 * i j e   where i and j are node IDs and e is passed to string2edge to produce the edge label.
 *)
let read ?(directed:bool=false) string2edge path =
	let g = create () in
	let f = open_in path in
	let line_regexp = Str.regexp "^[ \t]*\\([0-9]+\\)[ \t]*\\([0-9]+\\)[ \t]*\\(.*\\)$" in
	(try while true do
		let line = input_line f in
		if Str.string_match line_regexp line 0 then (
			let i     = int_of_string (Str.matched_group 1 line) in
			let j     = int_of_string (Str.matched_group 2 line) in
			let label = string2edge   (Str.matched_group 3 line) in
			if not (Hashtbl.mem g.nodes i) then	ignore (add_node_with_id g i ());
			if not (Hashtbl.mem g.nodes j) then	ignore (add_node_with_id g j ());
			if directed then
				add_directed_edge g i j label
			else
				add_edge g i j label
			)
		else
			raise (Failure (sprintf "Bad line in input graph file: \"%s\"\n" line))
		done
	with End_of_file -> ());
	close_in f;
	g

let write_to_file g edge2string file_path =
	let oc = open_out file_path in
	write g edge2string oc;
	close_out oc

let visualize
	?(undirected:bool=true)
	?(fmt:string = "ps")
	?(edge2neato:'b -> string = (fun _ -> ""))
	?(node2neato: ('a,'b) node -> string = (fun v -> sprintf "label=\"%d\"" v.id))
	g
	=
	let base_tmp_path = sprintf "/tmp/graph_visualization-%d" (Random.int 1000000000) in
	let dot_path = base_tmp_path ^ ".dot" in
	let dotfile = open_out dot_path in
	write_neato g node2neato edge2neato dotfile;
	close_out dotfile;
	let vis_path = base_tmp_path ^ "." ^ fmt in
	ignore (Sys.command(sprintf "neato -T%s %s >%s" fmt dot_path vis_path));
	if fmt = "ps" then
		ignore (Sys.command (sprintf "gv --spartan %s" vis_path))

(* Compute shortest paths from i to every other node.
 * Returns a hashtable mapping each node j in i's component to a tuple
 * (d, p) where d is the distance from i to j, and p is the
 * next-to-last hop along a shortest path from i to j.  If there
 * is no path i -> j, there will be no entry for j in the hashtable.
 * Thus the size of the returned hashtable is the size of i's component.
 * Node i's entry contains the tuple (0, i).
 *)
let shortest_paths g i =
	let dp = Hashtbl.create 1 in
	let get_dp i = hashtbl_find_default dp i (-1, -1) in
	let set_dp i d p = Hashtbl.replace dp i (d,p) in
	let q = Queue.create () in
	Hashtbl.replace dp i (0, i);
	Queue.push i q;
	while not (Queue.is_empty q) do
		let j = Queue.pop q in
		let j_node = find_node g j in
		let j_d, j_p = get_dp j in
		foreach_hashtbl j_node.edges (fun k _ ->
			let k_d, k_p = get_dp k in
			if k_d = -1 then (
				Queue.push k q;
				set_dp k (j_d + 1) j
				)
			)
		done;
	dp

(* Compute least-cost paths from i to every other node, where
 * edge costs are given in the hashtable `c' (mapping (id, id) pairs
 * to an integer cost).  Output format is exactly the same as
 * `shortest_paths' function above.
 *
 * usemax: if false, computes standard least-cost paths where
 * cost is sum of costs on edges.  If true, the cost is the *max*
 * cost of an edge used in the path.
 * 
 *)
 (* DEPRECATED VERSION -- use find_shortest_paths *)
let shortest_paths_weighted ?(usemax:bool=false) g i c =
	let aggregation_fn = if usemax then max else (+) in
	(* Store distance and predecessor pointer *)
	let dp = Hashtbl.create 1 in
	let get_dp dest = hashtbl_find_default dp dest (-1, -1) in
	let set_dp dest d p = Hashtbl.replace dp dest (d,p) in
	let q = Pqueue.make (fun (id1,dist1) (id2,dist2) ->
		compare dist2 dist1) (* smaller dists have higher priority *) in
	Hashtbl.replace dp i (0, i);
	Pqueue.enq q (i,0);
	let completed_nodes = Hashtbl.create 1 in
	while not (Pqueue.is_empty q) do
		let j, dist_in_queue = Pqueue.deq q in
		let j_node = find_node g j in
		let j_d, j_p = get_dp j in
		(* This node might have been added to the queue multiple
		 * times, and the version in the queue is not the one
		 * for the smallest distance for this node.  If so, ignore. *)
		assert (dist_in_queue >= j_d);
		if not (Hashtbl.mem completed_nodes j) then (
			Hashtbl.replace completed_nodes j ();
			foreach_hashtbl j_node.edges (fun k _ ->
				let k_d, k_p = get_dp k in
				let cost = Hashtbl.find c (j, k) in
				assert( cost >= 0 );
				let new_d = aggregation_fn j_d (Hashtbl.find c (j, k)) in (* sum or max *)
				if new_d < 0 then
					raise (Failure "Distance is negative: negative edges or overflow (paths too long?)");
				(* printf "    neighbor %d: curr dist = %d, parent = %d, new dist %d\n"
					k k_d k_p new_d; *)
				if k_d = -1 (* i.e., infinity *) or new_d < k_d then (
					Pqueue.enq q (k, new_d);
					set_dp k new_d j
					)
				)
			)
		(* else ignore *)
		done;
	dp

let find_shortest_paths
	g
	?(aggregation_fn:'b->'b->'b=(+.))  (* Usually this will be summing, but you could do e.g. max *)
	?(zero:'b = 0.0)                   (* Just what it sounds like *)
	?(infty:'b = infinity)             (* If using ints, can put -1 here. *)
	i                                  (* source node *)
	c                                  (* c: int -> int -> 'b  is a cost function: maps edge to cost *)
	=
	(* Store distance and predecessor pointer *)
	let dp = Hashtbl.create 1 in
	let get_dp dest = hashtbl_find_default dp dest (infty, -1) in
	let set_dp dest d p = Hashtbl.replace dp dest (d,p) in
	let q = Pqueue.make (fun (id1,dist1) (id2,dist2) ->
		compare dist2 dist1) (* smaller dists have higher priority *) in
	Hashtbl.replace dp i (zero, i);
	Pqueue.enq q (i,zero);
	let completed_nodes = Hashtbl.create 1 in
	while not (Pqueue.is_empty q) do
		let j, dist_in_queue = Pqueue.deq q in
		let j_node = find_node g j in
		let j_d, j_p = get_dp j in
		(* This node might have been added to the queue multiple
		 * times, and the version in the queue is not the one
		 * for the smallest distance for this node.  If so, ignore. *)
		assert (dist_in_queue >= j_d);
		if not (Hashtbl.mem completed_nodes j) then (
			Hashtbl.replace completed_nodes j ();
			foreach_hashtbl j_node.edges (fun k _ ->
				let k_d, k_p = get_dp k in
				let cost = c j k in
				assert( cost >= zero );
				let new_d = aggregation_fn j_d cost in
				if new_d < zero then
					raise (Failure "Distance is negative: negative edges or overflow (paths too long?)");
				if k_d = infty or new_d < k_d then (
					Pqueue.enq q (k, new_d);
					set_dp k new_d j
					)
				)
			)
		(* else ignore *)
		done;
	dp


(* Given a structure computed by `shortest_paths', trace back
 * the shortest path.  Returns a list of node IDs on the shortest path from
 * node `i' (the parameter to shortest_paths) to node `j'.
 * So `i' is the first and `j' is the last in the returned list.
 *)
let trace_path sp j =
	let path = ref [] in
	let k = ref j in
	let finished = ref false in
	while not !finished do
		(* if list_contains !k !path then (
			printf "Loop! Dest = %d, current = %d, path =" j !k;
			foreach !path (fun x -> printf " %d" x);
			printf "\n"; flush stdout;
			assert(false);
			); *) (* FIXME: can delete *)
		push path !k;
		let d, prev = Hashtbl.find sp !k in
		if !k = prev then
			finished := true
		else
			k := prev;
		done;
	!path

(* Given a graph g and a list of nodes, return a new graph which is the subgraph
 * of `g' induced by `nodes'. The node IDs will be the same, and lowest_unused_id
 * will also be the same as in g.  Ignores any `nodes' that do not appear in
 * `g.nodes'.
 * WARNING: untested.
 *)
let subgraph g nodes =
	let h = create () in
	h.lowest_unused_id <- g.lowest_unused_id;
	(* Copy nodes *)
	foreach nodes (fun i ->
		(* make sure no duplicates *)
		if (contains_node g i) && (not (contains_node h i)) then (
			let v = find_node g i in
			ignore (add_node_with_id h i v.label)
			)
		);
	(* Copy edges *)
	foreach_hashtbl h.nodes (fun i v ->
		let g_node = find_node g i in
		foreach_hashtbl g_node.edges (fun j label ->
			if Hashtbl.mem h.nodes j then
				Hashtbl.add v.edges j label
			)
		);
	h

(* Finds the connected components of g, assuming a undirected graph.
 * For digraphs, behavior is undefined.  Returns a list of components,
 * each of which is a list of the node identifiers in the component.
 *)
let connected_components g =
	let remaining_nodes = Hashtbl.create 1 in
	(* Annoying.  We just want to retrieve any one element of the hashtable,
	 * and it takes all this code. *)
	let get_one () =
		let one = ref None in
		try (
			foreach_hashtbl remaining_nodes (fun i _ ->
				one := Some i; raise Not_found); (* not really Not_found... *)
			raise Not_found
			)
		with _ -> match !one with
			None -> raise Not_found
			| Some i -> i
		in
	foreach_hashtbl g.nodes (fun i _ ->
		Hashtbl.add remaining_nodes i ());
	let components = ref [] in
	while Hashtbl.length remaining_nodes > 0 do
		let i = get_one () in
		let component = hashtbl_keys (shortest_paths g i) in
		push components component;
		(* Remove each member of the component from `remaining_nodes' *)
		foreach component (Hashtbl.remove remaining_nodes)
		done;
	!components

let largest_connected_subgraph g =
	let components = connected_components g in
	let biggest = argmax components List.length in
	subgraph g biggest

(* Create a random graph from the G(n,m) model.
 * FIXME: when nearly all edges are included, this will be slow.  We should
 * flip it around and pick the edges to *not* add in that case. *)
let generate_gnm n m =
	let g = create () in
	for i = 0 to n-1 do
		ignore (add_node_with_id g i ())
		done;
	let edges_already_added = Hashtbl.create 1 in
	let i = ref 0 in
	while !i < m do
		let v = Random.int n in
		let w = (v + Random.int (n-1)) mod n in
		let e = if v < w then (v,w) else (w,v) in (* canonicalize ordering *)
		if not (Hashtbl.mem edges_already_added e) then (
			Hashtbl.add edges_already_added e ();
			incr i;
			add_edge g v w ()
			)
		done;
	g

(* Create a regular random graph with degree d on n nodes.
 * Not guaranteed to be drawn from the uniform distribution over such graphs;
 * that's difficult to do.
 *)
let generate_regular_random_graph node_label n d =
	let g = create () in
	for i = 0 to n-1 do
		ignore (add_node_with_id g i node_label)
		done;
	let edges_already_added = Hashtbl.create 1 in
	let candidate_is_good (i,j) =
		(i != j) && (not (Hashtbl.mem edges_already_added (i,j)))
		in
	(* Array of endpoints that are unmatched so far *)
	let endpoints = ref (Array.init (n*d) (fun i -> i mod n)) in
	for try_num = 1 to 4 do
		ignore (permute_arr !endpoints);
		let unfinished = ref [] in
		let i = ref 0 in
		while !i <= Array.length !endpoints - 2 do
			let j = !endpoints.(!i) in
			let k = !endpoints.(!i+1) in
			let e = if j < k then (j,k) else (k,j) in (* canonicalize ordering *)
			if candidate_is_good e then (
				add_edge g j k ();
				Hashtbl.add edges_already_added e ();
				i := !i + 2
				)
			else (
				push unfinished j;
				i := !i + 1
				)
			done;
		if !i = Array.length !endpoints - 1 then
			(* odd one out *)
			push unfinished !endpoints.(!i);
		
		(* Prepare the unfinished endpoints to be handled next time around *)
		endpoints := Array.of_list !unfinished;
		done;
	let num_unfinished = Array.length !endpoints in
	if num_unfinished > 0 then (
		printf "generate_regular_random_graph: warning: %d endpoints unmatched\n"
			num_unfinished;
		flush stdout
		);
	g

(* Create a random graph with given degree distribution.  The idea is
 * to draw from the uniform distribution over such graphs, but actually,
 * that's probably difficult.  We don't guarantee any particular distribution,
 * but it's probably roughly uniform.
 * Occasionally, some nodes will have slightly less degree than they should.
 * `d_dist' is an array of nodes' degrees.
 *)
let rec generate_random_graph node_label d_dist =
	let n = Array.length d_dist in
	let g = create () in
	for i = 0 to n-1 do
		ignore (add_node_with_id g i node_label)
		done;
	let edges_already_added = Hashtbl.create 1 in
	let candidate_is_good (i,j) =
		(i != j) && (not (Hashtbl.mem edges_already_added (i,j)))
		in
	
	let endpoints = ref [] in
	for i = 0 to n-1 do
		for j = 1 to d_dist.(i) do
			push endpoints i
			done
		done;

	let endpoints = ref (Array.of_list !endpoints) in
	for try_num = 1 to 4 do
		ignore (permute_arr !endpoints);
		let unfinished = ref [] in
		let i = ref 0 in
		while !i <= Array.length !endpoints - 2 do
			let j = !endpoints.(!i) in
			let k = !endpoints.(!i+1) in
			let e = if j < k then (j,k) else (k,j) in (* canonicalize ordering *)
			if candidate_is_good e then (
				add_edge g j k ();
				Hashtbl.add edges_already_added e ();
				i := !i + 2
				)
			else (
				push unfinished j;
				i := !i + 1
				)
			done;
		if !i = Array.length !endpoints - 1 then
			(* odd one out *)
			push unfinished !endpoints.(!i);
		
		(* Prepare the unfinished endpoints to be handled next time around *)
		endpoints := Array.of_list !unfinished;
		done;
	let num_unfinished = Array.length !endpoints in
	if num_unfinished > 0 then (
		printf "generate_regular_random_graph: warning: %d endpoints unmatched; retrying\n"
			num_unfinished;
		flush stdout;
		generate_random_graph node_label d_dist
		)
	else
		g

(* Generate a geometric random graph:  n nodes at uniform-random locations
 * in the unit square [0,1] x [0,1].  An edge exists if two nodes are at
 * distance <= r, where r = sqrt(d / ((n-1)*pi)), which produces mean
 * degree d (except for nodes near the border).
 * Node labels are the (x,y) coordinates.  Edge labels are distance.
 *)
let generate_geometric_random_graph n mean_degree =
	let r = sqrt(mean_degree /. (i2f (n - 1) *. 3.14159265359)) in

	(* Bucketing of nodes by area. Buckets correspond to
	 * square tiles of size r x r.  *)
	let buckets = Hashtbl.create 1 in
	let m = f2i (ceil (1. /. r)) in (* number of buckets on a side *)
	(* Compute the bucket position of a node *)
	let bucket_pos v =
		let x = fst v.label in
		let y = snd v.label in
		(f2i (x /. i2f m)), (f2i (y /. i2f m))
		in

	(* Create nodes, and put in buckets according to location. *)
	let g = create () in
	for i = 0 to n-1 do
		let pos = (Random.float 1.0, Random.float 1.0) in
		let v = add_node g pos in
		let bpos = bucket_pos v in
		Hashtbl.replace buckets bpos (v :: (hashtbl_find_default buckets bpos []))
		done;

	(* Build edges: For each node, look in the <= 9 nearby tiles
	 * which will find all nodes that could be neighbors. *)
	foreach_hashtbl g.nodes (fun id v ->
		let (bx, by) = bucket_pos v in
		let possible_neighbor_buckets = [
			(bx-1,by+1); (bx,  by+1); (bx+1,by+1);
			(bx-1,by  ); (bx,  by  ); (bx+1,by  );
			(bx-1,by-1); (bx,  by-1); (bx+1,by-1);]
			in
		foreach possible_neighbor_buckets (fun b ->
			foreach (hashtbl_find_default buckets b []) (fun w ->
				(* If this node is close, and not us, create edge to it *)
				let distance = sqrt((fst v.label -. fst w.label)**2.0 +. (snd v.label -. snd w.label)**2.0) in
				if (distance <= r) && (v.id <> w.id) then
					add_directed_edge g v.id w.id distance
				)
			)
		);
	g

(* Visualizes a geometric graph: Node labels are (x,y) : float*float positions.
 * Edge labels are ignored.  We only print one edge in each direction, so
 * if you are using the directedness of the graph, this might not be good. *)
let visualize_geometric_graph g =
	let dat = open_out "/tmp/grg_vis.dat" in
	fprintf dat "# Nodes\n";
	foreach_hashtbl g.nodes (fun id v ->
		fprintf dat "%f %f\n\n" (fst v.label) (snd v.label)
		);
	fprintf dat "\n# Edges\n";
	foreach_hashtbl g.nodes (fun i v ->
		foreach_hashtbl v.edges (fun j _ ->
			let w = find_node g j in
			if i < j then
				fprintf dat "%f %f\n%f %f\n\n" (fst v.label) (snd v.label) (fst w.label) (snd w.label)
			)
		);
	close_out dat;
	
	let gpl = open_out "/tmp/grg_vis.gpl" in
	fprintf gpl "
		set terminal pdf
		set key bottom right
		set size square
		set grid
		set output \"/tmp/grg_vis.pdf\"
		set key off
		set yrange [0:1]
		set xrange [0:1]
		plot \"/tmp/grg_vis.dat\" index 1 w l, \"\" index 0 w p
		";
	close_out gpl;
	ignore (Sys.command("gnuplot /tmp/grg_vis.gpl"));
	ignore (Sys.command("open /tmp/grg_vis.pdf"))
