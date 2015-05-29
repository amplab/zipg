open Util
open Printf

type 'a t = {
	comparator : 'a option -> 'a option -> int;
	mutable n : int;
	mutable els : 'a option array
	}

exception Empty

(*********** For internal use only **************)
let append q el =
	if Array.length q.els <= q.n then
		q.els <- Array.init (max 1 (2*q.n)) (fun i ->
			if i < q.n then
				q.els.(i)
			else
				None);
	q.els.(q.n) <- Some el;
	q.n <- q.n + 1

let remove_last q =
	q.els.(q.n-1) <- None;
	q.n <- q.n - 1;
	if q.n <= Array.length q.els / 4 then
		q.els <- Array.init (2*q.n) (fun i -> q.els.(i))

let parent i = (i-1)/2
let lchild i = 2*i + 1
let rchild i = 2*i + 2

(* Return the element index which should be closer to the top of the heap. *)
let topmost q el_indeces =
	arg_extremum q.comparator el_indeces (fun i -> q.els.(i))

let rec reheap_up q i =
	let p = parent i in
	if i > 0 && i = topmost q [p;i] then (
		swap q.els p i;
		reheap_up q p
		)

let rec reheap_down q i =
	if lchild i < q.n then
		let e = topmost q [i; lchild i; rchild i] in
		if i != e then (
			swap q.els i e;
			reheap_down q e
			)

(*********** Interface functions **************)

(* Comparator returns 1 if the first argument is higher-priority. *)
let make comparator =
	{comparator = (fun a b ->
		match a,b with
			Some a, Some b -> comparator a b
			| Some a, None -> 1
			| None, Some b -> -1
			| None,None -> 0);
	n=0; els=Array.make 1 None}

let length q =
	q.n

let is_empty q =
	q.n = 0

let enq q el =
	append q el;
	reheap_up q (q.n-1)

let deq q =
	if q.n = 0 then
		raise Empty
	else (
		let e = opt2exn q.els.(0) in
		swap q.els 0 (q.n-1);
		remove_last q;
		reheap_down q 0;
		e
		)

let peek q =
	if q.n = 0 then
		raise Empty
	else (
		match q.els.(0) with
			Some e -> e
			| None -> assert false (* we already checked q.n > 0 ! *)
		)

let iter q f =
	for i = 0 to q.n-1 do
		f (opt2exn q.els.(i))
		done

let clear q =
	q.n <- 0;
	q.els <- Array.make 1 None

let printdebug q el2string =
	printf "[";
	for i = 0 to Array.length q.els - 1 do
		match q.els.(i) with
			Some x -> printf "%s " (el2string x)
			| None -> printf "N "
		done;
	printf "]"

(*********** Simple interactive tester **************)
(*
let main =
	let q = make compare in
	while true do
		printf "Current queue: ";
		for i = 0 to Array.length q.els - 1 do
			match q.els.(i) with
				Some x -> printf "%d " x
				| None -> printf "N "
			done;
		printf "\n> ";
		let command = read_int () in
		if command = 0 then
			try
				let x = deq q in
				printf "dequeued %d\n" x
			with Empty ->
				printf "Empty queue!\n"
		else if command = 1 then
			let x = read_int () in
			enq q x
		else
			printf "Invalid command\n"
		done
*)
