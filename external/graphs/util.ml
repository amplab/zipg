(************************************************************************
 * Author:  Brighten Godfrey <pbg@cs.berkeley.edu>
 ************************************************************************)

(************************************************************************
 * Basic utilities
 ************************************************************************)

open Printf

let i2f = float_of_int
let f2i = int_of_float
let s2f = float_of_string

let round x = f2i (x +. 0.5)

let uninitialized = fun _ -> raise (Failure "Uninitialized object")

let list_empty lst =
	match lst with
		x::_ -> false
		| [] -> true

let incr_int64 x =
	x := Int64.add 1L !x

let opt2exn x =
	match x with
		Some y -> y
		| None -> raise (Failure "Tried to get Some from None!")

let swap arr i j =
	let tmp = arr.(i) in
	arr.(i) <- arr.(j);
	arr.(j) <- tmp

let permute_arr a =
	let n = Array.length a in
	for i = 0 to n - 1 do
		let j = Random.int (n - i) in
		swap a i (i+j)
		done;
	a

(* Returns permutation of {0, ..., n-1} *)
let permutation n =
	permute_arr (Array.init n (fun i -> i))

let log_base b x = (log x) /. (log b)
let log_2 = log_base 2.0

(* Insert <separator> between all entries of <lst> *)
let rec separate lst separator =
	match lst with
    	[] -> []
        | x::[] -> [x]
        | x::y::rest -> x::separator::(separate (y::rest) separator)

(* Print a list of strings separated by <separator> *)
let print_list lst separator =
	List.fold_left (^) "" (separate lst separator)

(* list of all sublists of lst *)
let rec power_set lst =
	match lst with
    	[] ->
        	[ [] ]
        | a :: lst ->
        	let ps_no_a = power_set lst in
            let ps_a = List.map (fun x -> a::x) ps_no_a in
            ps_no_a @ ps_a

let arg_extremum comparator args f =
	assert(args != []);
	let argext, _ = List.fold_left (fun (a,f_a) b ->
		let f_b = f b in
		if comparator f_a f_b > 0 then
			(a,f_a)
		else
			(b,f_b))
		(List.hd args, f (List.hd args)) (List.tl args)
		in
	argext
let argmax (args:'a list) (f:'a->'b) =
	arg_extremum (compare:'b->'b->int) args f
let argmin (args:'a list) (f:'a->'b) =
	arg_extremum (fun (a:'b) (b:'b) -> compare b a) args f


let output_of_command cmd =
	let proc_in = Unix.open_process_in cmd in
	let output  = ref "" in
	(try while true do
		output := !output ^ (input_line proc_in) ^ "\n";
		done
	with End_of_file -> ()
	);
	ignore (Unix.close_process_in proc_in);
	!output

(* Equivalent to [f 0; f 1; f 2; ... ; f (n-1)].
 * Assumes n > 0.
 *)
let make_list f n =
	let rec make_list_aux lst i =
		if i < 0 then
			lst
		else
			make_list_aux ((f i)::lst) (i-1)
		in
	make_list_aux [] (n-1)

let foreach lst f =        List.iter    f lst
let foreach2 lst1 lst2 f = List.iter2   f lst1 lst2
let foreach_arr arr f =    Array.iter   f arr
let foreach_hashtbl h f =  Hashtbl.iter f h
let foreach_line stream f =
	try while true do
		f (input_line stream)
		done
	with End_of_file -> ()

let map lst f = List.map f lst
let map2 lst1 lst2 f = List.map2 f lst1 lst2
let rev_map lst f = List.rev_map f lst

(* foreach_i [x1;x2;...;xn] f   is equivalent to
 * f 0 x1; f 1 x2; ... ; f (n-1) xn
 *)
let foreach_i lst f =
	ignore (List.fold_left (fun i x -> f i x; i+1) 0 lst)

let list_contains el lst =
	List.exists (fun x -> x = el) lst

let list_find_min compare_fn lst =
	List.fold_left (fun min_so_far element ->
		if compare_fn element min_so_far < 0 then
			element
		else
			min_so_far
		) (List.hd lst) (List.tl lst)

let push list_ref element =
	list_ref := element :: !list_ref

let pop list_ref =
	let element = List.hd !list_ref in
	list_ref := List.tl !list_ref;
	element

let matches str regexp =
	Str.string_match (Str.regexp regexp) str 0

(* Remove leading and trailing whitespace from a string. *)
let chomp str =
	let str = Str.replace_first (Str.regexp "^[ \t\n\r]+") "" str in
	Str.replace_first (Str.regexp "[ \t\n\r]+$") "" str

let marshal structure pathname =
	let f = open_out_bin pathname in
	Marshal.to_channel f structure [];
	close_out f

let unmarshal pathname =
	let f = open_in_bin pathname in
	let x = Marshal.from_channel f in
	close_in f;
	x

let load_cacheable_object path cache_path parse_fn =
	(try (if (Unix.stat path).Unix.st_mtime > (Unix.stat cache_path).Unix.st_mtime then
		Unix.unlink cache_path)
	with _ -> ());
	try
		unmarshal cache_path
	with _ -> (
		let infile = open_in path in
		let o = parse_fn infile in
		close_in infile;
		marshal o cache_path;
		o
		)

(************************************************************************
 * Hashtable extensions
 ************************************************************************)

(* Return an unordered list of the keys in a hashtable.
 * If the same key has been added multiple times, then it appears here
 * multiple times. *)
let hashtbl_keys ht =
	Hashtbl.fold (fun key data keys -> key::keys) ht []

(* Return an unordered list of the values in a hashtable. *)
let hashtbl_values ht =
	Hashtbl.fold (fun key v values -> v::values) ht []

(* Return an unordered list of (key, value) pairs in a hashtable.
 * If the same key has been added multiple times, then it appears here
 * multiple times. *)
let hashtbl_to_list ht =
	Hashtbl.fold (fun key data lst -> (key,data)::lst) ht []

let hashtbl_of_list lst =
	let h = Hashtbl.create 1 in
	foreach lst (fun (k,v) ->
		Hashtbl.add h k v);
	h

(* Find an element in a hashtable, or return the given default. *)
let hashtbl_find_default ht key default =
	try
		Hashtbl.find ht key
	with Not_found ->
		default

(* The number of bindings in the hashtable. May be greater than the number
 * of unique keys. *)
let hashtbl_size ht =
	List.length (hashtbl_keys ht)


let hashtbl_pick_any h = 
	let res = ref None in
	try
		Hashtbl.iter (fun k v -> res := Some (k,v); raise Exit) h;
		raise Not_found
	with Exit -> match !res with
		| Some x -> x
		| None -> raise (Failure "Bug in util.ml?")

(************************************************************************
 * Statistics
 ************************************************************************)

(* sum of a list *)
let sum_floats = List.fold_left (+.) 0.0
let sum_ints = List.fold_left (+) 0

(* FIXME: upgrade to O(n)-time algorithm. *)
let median_ints lst =
	let lst = List.sort compare lst in
	let n = List.length lst in
	if n mod 2 = 1 then
		i2f (List.nth lst (n / 2))
	else
		i2f ((List.nth lst (n/2)) + (List.nth lst (-1+n/2))) /. 2.

(* WARNING: USES MAP, NOT TAIL RECURSIVE! *)
let avg num_trials fn_to_avg =
	let sum = ref 0.0 in
    for i = 1 to num_trials do
    	sum := !sum +. (fn_to_avg ())
        done;
    !sum /. (float_of_int num_trials)
let avg_list num_trials fn_to_avg =
	let sums = ref (fn_to_avg ()) in
    for i = 1 to num_trials - 1 do
    	sums := List.map2 (fun a b -> a+.b) !sums (fn_to_avg())
        done;
    List.map (fun sum -> sum /. (float_of_int num_trials)) !sums

let mean_stddev data =
	let n = i2f (List.length data) in
	let mean = (sum_floats data) /. n in
	let sum_sq = ref 0.0 in
	foreach data (fun x -> sum_sq := (x -. mean)**2. +. !sum_sq);
	let var = !sum_sq /. (n-.1.) in
	mean, (sqrt var)

class stats = object(self)
	val mutable cnt = 0
	val mutable sum = 0.0
	val mutable sumsq = 0.0
	val mutable max_sample = neg_infinity
	val mutable min_sample = infinity
	method add_sample x =
		cnt <- cnt + 1;
		sum <- sum +. x;
		sumsq <- sumsq +. (x *. x);
		max_sample <- max x max_sample;
		min_sample <- min x min_sample
	method count = cnt
	method mean = sum /. i2f cnt
	method variance = sumsq -. (self#mean ** 2.0)
	method stddev = sqrt self#variance
	method min = min_sample
	method max = max_sample
	method sum = sum
	end

(* Values of Student's t-distribution, taken from
 * http://www.itl.nist.gov/div898/handbook/eda/section3/eda3672.htm.
 * Rows correspond to degrees of freedom (number of samples - 1),
 * ranging from 1 to 100.  So if you took 10 samples, the index of the
 * array you should use is 8.
 * Columns, left to right:
 *     * 20% two-sided error
 *     * 10% two-sided error
 *     *  5% two-sided error
 *     *  2% two-sided error
 *     *  1% two-sided error
 *     *  0.2% two-sided error
 *)
let students_t_distribution_values = [|
	[|      3.078;   6.314;  12.706;  31.821;  63.657; 318.313|];
	[|      1.886;   2.920;   4.303;   6.965;   9.925;  22.327|];
	[|      1.638;   2.353;   3.182;   4.541;   5.841;  10.215|];
	[|      1.533;   2.132;   2.776;   3.747;   4.604;   7.173|];
	[|      1.476;   2.015;   2.571;   3.365;   4.032;   5.893|];
	[|      1.440;   1.943;   2.447;   3.143;   3.707;   5.208|];
	[|      1.415;   1.895;   2.365;   2.998;   3.499;   4.782|];
	[|      1.397;   1.860;   2.306;   2.896;   3.355;   4.499|];
	[|      1.383;   1.833;   2.262;   2.821;   3.250;   4.296|];
	[|      1.372;   1.812;   2.228;   2.764;   3.169;   4.143|];
	[|      1.363;   1.796;   2.201;   2.718;   3.106;   4.024|];
	[|      1.356;   1.782;   2.179;   2.681;   3.055;   3.929|];
	[|      1.350;   1.771;   2.160;   2.650;   3.012;   3.852|];
	[|      1.345;   1.761;   2.145;   2.624;   2.977;   3.787|];
	[|      1.341;   1.753;   2.131;   2.602;   2.947;   3.733|];
	[|      1.337;   1.746;   2.120;   2.583;   2.921;   3.686|];
	[|      1.333;   1.740;   2.110;   2.567;   2.898;   3.646|];
	[|      1.330;   1.734;   2.101;   2.552;   2.878;   3.610|];
	[|      1.328;   1.729;   2.093;   2.539;   2.861;   3.579|];
	[|      1.325;   1.725;   2.086;   2.528;   2.845;   3.552|];
	[|      1.323;   1.721;   2.080;   2.518;   2.831;   3.527|];
	[|      1.321;   1.717;   2.074;   2.508;   2.819;   3.505|];
	[|      1.319;   1.714;   2.069;   2.500;   2.807;   3.485|];
	[|      1.318;   1.711;   2.064;   2.492;   2.797;   3.467|];
	[|      1.316;   1.708;   2.060;   2.485;   2.787;   3.450|];
	[|      1.315;   1.706;   2.056;   2.479;   2.779;   3.435|];
	[|      1.314;   1.703;   2.052;   2.473;   2.771;   3.421|];
	[|      1.313;   1.701;   2.048;   2.467;   2.763;   3.408|];
	[|      1.311;   1.699;   2.045;   2.462;   2.756;   3.396|];
	[|      1.310;   1.697;   2.042;   2.457;   2.750;   3.385|];
	[|      1.309;   1.696;   2.040;   2.453;   2.744;   3.375|];
	[|      1.309;   1.694;   2.037;   2.449;   2.738;   3.365|];
	[|      1.308;   1.692;   2.035;   2.445;   2.733;   3.356|];
	[|      1.307;   1.691;   2.032;   2.441;   2.728;   3.348|];
	[|      1.306;   1.690;   2.030;   2.438;   2.724;   3.340|];
	[|      1.306;   1.688;   2.028;   2.434;   2.719;   3.333|];
	[|      1.305;   1.687;   2.026;   2.431;   2.715;   3.326|];
	[|      1.304;   1.686;   2.024;   2.429;   2.712;   3.319|];
	[|      1.304;   1.685;   2.023;   2.426;   2.708;   3.313|];
	[|      1.303;   1.684;   2.021;   2.423;   2.704;   3.307|];
	[|      1.303;   1.683;   2.020;   2.421;   2.701;   3.301|];
	[|      1.302;   1.682;   2.018;   2.418;   2.698;   3.296|];
	[|      1.302;   1.681;   2.017;   2.416;   2.695;   3.291|];
	[|      1.301;   1.680;   2.015;   2.414;   2.692;   3.286|];
	[|      1.301;   1.679;   2.014;   2.412;   2.690;   3.281|];
	[|      1.300;   1.679;   2.013;   2.410;   2.687;   3.277|];
	[|      1.300;   1.678;   2.012;   2.408;   2.685;   3.273|];
	[|      1.299;   1.677;   2.011;   2.407;   2.682;   3.269|];
	[|      1.299;   1.677;   2.010;   2.405;   2.680;   3.265|];
	[|      1.299;   1.676;   2.009;   2.403;   2.678;   3.261|];
	[|      1.298;   1.675;   2.008;   2.402;   2.676;   3.258|];
	[|      1.298;   1.675;   2.007;   2.400;   2.674;   3.255|];
	[|      1.298;   1.674;   2.006;   2.399;   2.672;   3.251|];
	[|      1.297;   1.674;   2.005;   2.397;   2.670;   3.248|];
	[|      1.297;   1.673;   2.004;   2.396;   2.668;   3.245|];
	[|      1.297;   1.673;   2.003;   2.395;   2.667;   3.242|];
	[|      1.297;   1.672;   2.002;   2.394;   2.665;   3.239|];
	[|      1.296;   1.672;   2.002;   2.392;   2.663;   3.237|];
	[|      1.296;   1.671;   2.001;   2.391;   2.662;   3.234|];
	[|      1.296;   1.671;   2.000;   2.390;   2.660;   3.232|];
	[|      1.296;   1.670;   2.000;   2.389;   2.659;   3.229|];
	[|      1.295;   1.670;   1.999;   2.388;   2.657;   3.227|];
	[|      1.295;   1.669;   1.998;   2.387;   2.656;   3.225|];
	[|      1.295;   1.669;   1.998;   2.386;   2.655;   3.223|];
	[|      1.295;   1.669;   1.997;   2.385;   2.654;   3.220|];
	[|      1.295;   1.668;   1.997;   2.384;   2.652;   3.218|];
	[|      1.294;   1.668;   1.996;   2.383;   2.651;   3.216|];
	[|      1.294;   1.668;   1.995;   2.382;   2.650;   3.214|];
	[|      1.294;   1.667;   1.995;   2.382;   2.649;   3.213|];
	[|      1.294;   1.667;   1.994;   2.381;   2.648;   3.211|];
	[|      1.294;   1.667;   1.994;   2.380;   2.647;   3.209|];
	[|      1.293;   1.666;   1.993;   2.379;   2.646;   3.207|];
	[|      1.293;   1.666;   1.993;   2.379;   2.645;   3.206|];
	[|      1.293;   1.666;   1.993;   2.378;   2.644;   3.204|];
	[|      1.293;   1.665;   1.992;   2.377;   2.643;   3.202|];
	[|      1.293;   1.665;   1.992;   2.376;   2.642;   3.201|];
	[|      1.293;   1.665;   1.991;   2.376;   2.641;   3.199|];
	[|      1.292;   1.665;   1.991;   2.375;   2.640;   3.198|];
	[|      1.292;   1.664;   1.990;   2.374;   2.640;   3.197|];
	[|      1.292;   1.664;   1.990;   2.374;   2.639;   3.195|];
	[|      1.292;   1.664;   1.990;   2.373;   2.638;   3.194|];
	[|      1.292;   1.664;   1.989;   2.373;   2.637;   3.193|];
	[|      1.292;   1.663;   1.989;   2.372;   2.636;   3.191|];
	[|      1.292;   1.663;   1.989;   2.372;   2.636;   3.190|];
	[|      1.292;   1.663;   1.988;   2.371;   2.635;   3.189|];
	[|      1.291;   1.663;   1.988;   2.370;   2.634;   3.188|];
	[|      1.291;   1.663;   1.988;   2.370;   2.634;   3.187|];
	[|      1.291;   1.662;   1.987;   2.369;   2.633;   3.185|];
	[|      1.291;   1.662;   1.987;   2.369;   2.632;   3.184|];
	[|      1.291;   1.662;   1.987;   2.368;   2.632;   3.183|];
	[|      1.291;   1.662;   1.986;   2.368;   2.631;   3.182|];
	[|      1.291;   1.662;   1.986;   2.368;   2.630;   3.181|];
	[|      1.291;   1.661;   1.986;   2.367;   2.630;   3.180|];
	[|      1.291;   1.661;   1.986;   2.367;   2.629;   3.179|];
	[|      1.291;   1.661;   1.985;   2.366;   2.629;   3.178|];
	[|      1.290;   1.661;   1.985;   2.366;   2.628;   3.177|];
	[|      1.290;   1.661;   1.985;   2.365;   2.627;   3.176|];
	[|      1.290;   1.661;   1.984;   2.365;   2.627;   3.175|];
	[|      1.290;   1.660;   1.984;   2.365;   2.626;   3.175|];
	[|      1.290;   1.660;   1.984;   2.364;   2.626;   3.174|];
	|]

(* Returns the value of Student's t-distribution to 3 decimal places for
 * the given level of two-sided confidence and the given number of samples.
 * num_samples must be at least 2.  If it is greater than 101,
 * the function returns the value for num_samples = 101, thus taking a
 * slightly pessimistic view.  The confidence level must be one of
 * 0.8, 0.9, 0.95, 0.98, 0.99, 0.998.
 *)
let students_t_distribution num_samples confidence =
	if num_samples < 2 then
		raise (Failure (sprintf "Number of samples is %d but should be >= 2 to compute confidence intervals."
			num_samples
		));
	let row = min (num_samples - 2) 99 in
	let col = match confidence with
		0.8 -> 0
		| 0.9 -> 1
		| 0.95 -> 2
		| 0.98 -> 3
		| 0.99 -> 4
		| 0.998 -> 5
		| _ -> raise (Failure "Can't handle that confidence interval!")
		in
	students_t_distribution_values.(row).(col)

let mean_confidence confidence data =
	let mean, stddev = mean_stddev data in
	let num_trials = List.length data in
	let d = students_t_distribution num_trials confidence in
	(mean, d *. stddev /. sqrt (i2f num_trials))

(* Removes "special" characters from string s, which would be bad to
 * have in a filename: anything other than A-Za-z0-9... *)
let remove_special s =
    Str.global_replace (Str.regexp "[^A-Za-z0-9\\._\\-]") "_" s

(* Plots (f var1 var2 i) as a function of var1, for various values of var2,
 * where `i' is the trial number ranging from 0 to `num_trials' - 1.
 * Data output files are named <base_outfile>-<var1name>-<var2name>.dat (NO!)
 * Format is 
 * FIXME: should accept non-float arguments
 * FIXME: doesn't deal with unusual characters in filenames well
 *
 * A note on data types.
 *
 * There are two dimensions of input values (var1, var2), and
 * multiple dimensions of output values (the function essentially returns a
 * vector of arbitrary dimension -- implemented as a list.)
 *
 * We need a name for each dimension, and for each value in each dimension.
 *
 * Input var1:
 *     * Dimension name given as string in argument var1name
 *     * Dimension type 'b is arbitrary
 *     * Values given as 'b list
 *     * Value names given as mapping var1to_string: 'b -> string
 * Input var2:
 *     * Dimension name given as string in argument var2name
 *     * Dimension type 'a is arbitrary
 *     * Values and value names given as list of pairs (string, 'a)
 * Each output dimension:
 *     * Name returned by argument f
 *     * Type float (must be eventually averaged, so no generality is lost)
 *     * Values returned by f
 *     * Value names can be extracted by string_of_float
 *
 * Question: why handle var1 and var2 value names differently?
 *     var1: mapping       var2: enumeration
 *)
type what_to_plot_t = PlotAll | PlotSome of (int*int) list | PlotCDF

let stats ?(num_trials: int          = 1)
          ?(num_trials_fnvar1: ('b->int) option = None)  (* can optionally specify # trials as fn of var1 *)
          ?(confidence: float option = None)
          ?(gnuplot_options: string  = "")
		  ?(plots:what_to_plot_t     = PlotAll)
		  ?(show_plots:bool          = true)
          ~(f:'b -> 'a -> int -> (string*float) list)
          ~(base_outfile)
          ~(var1name: string)
		  ~(var1domain: 'b list)
		  ~(var1to_string: 'b -> string)
          ~(var2name: string)
          ~(var2domain: (string * 'a) list) () =
	(* Write raw data file *)
	let outfile = open_out (base_outfile ^ ".dat") in
	let num_stats = ref (-1) in
	let stat_names = ref [] in
	let print_header () =
		let f, o = fprintf, outfile in
		f o "#\n# Created automatically by Brighten's stats program.\n";
		f o "# There is one section per %s, separated by two newlines;\n" var2name;
		f o "# select this using `index' in gnuplot:\n";
		foreach_i var2domain (fun i (name,_) -> f o "#     %d  %s\n" i name);
		f o "#\n# Columns, selected with `using' in gnuplot:\n#     1  %s\n" var1name;
		foreach_i !stat_names (fun i name ->
			f o "#     %d  %s\n" (i*2+2) name;
			match confidence with None ->
				f o "#     %d  N/A\n"
					(i*2+3)
			| Some c ->
				f o "#     %d  %s %0.1f confidence delta\n" (i*2+3) name
					(100.*.c)
			);
		f o "#\n\n"
		in
	foreach var2domain (fun (var2valname, var2val) ->
    	printf "%s = %s; %s = " var2name var2valname var1name; flush stdout;
		let var1_n = List.length var1domain in
		(* If we're supposed to plot a CDF, here we'll store the data for
		 * each "var1domain", which we will sort after gathering it all.
		 * Format: There is one array element per var1 value (but the matching
		 * from elements to var1 values is lost).  Each element contains
		 * an array, with one entry per statistic outputted by `f'.  Finally,
		 * each one of those entries is a pair of (mean, confidence interval).
		 *)
		let cdf_data_points = Array.init var1_n (fun i -> [| |]) in
    	foreach_i var1domain (fun var1index var1val ->
			if var1index = 0 then
            	printf "%s" (var1to_string var1val)
			else
				printf ", %s" (var1to_string var1val);
			flush stdout;
			let all_samples = ref [] in
(*
			let raw_data = Array.create num_trials [] in
			(* Get the data in num_trials threads *)
			for i = 1 to num_trials do
				
				done;
*)
			let num_trials =
				match num_trials_fnvar1 with
					None ->	num_trials
					| Some fn -> fn var1val
				in
			for i = 1 to num_trials do
				(* Get a data point (for each statistic) *)
				let names, samples = List.split (f var1val var2val (i-1)) in
				(* One-time setup stuff *)
				if !stat_names = [] then (
					stat_names := names;
					num_stats := List.length names;
					print_header ()
					)
				else
					assert(List.length names = !num_stats);
				if i = 1 then (
					all_samples := make_list (fun _ -> ref []) !num_stats;
					if var1index = 0 then
				    	fprintf outfile "# %s = %s\n" var2name var2valname
					);
				assert(names = !stat_names);
				(* Record this data point *)
				foreach2 samples !all_samples (fun datum data ->
					data := datum::!data
					);
				printf "."; flush stdout;
				done;
			(* Compute summary statistics over the trials *)
			let summary = List.map (fun data ->
				match confidence with
					None ->
						(fst (mean_stddev !data)), 0.0
					| Some x ->
						mean_confidence x !data
				) !all_samples
				in
			if plots = PlotCDF then
				(* Save the data, we'll sort it later *)
				cdf_data_points.(var1index) <- Array.of_list summary
			else (
				(* Output the data now *)
				fprintf outfile "%12s" (var1to_string var1val);
				foreach summary (fun (mean, confidence_interval) ->
					fprintf outfile " %12g %12g" mean confidence_interval
					);
				fprintf outfile "\n";
	            flush outfile;
				)
            );
		if plots = PlotCDF then (
			let sorted_map = Array.init !num_stats (fun i -> Array.init var1_n
				(fun j -> j)) in
			for i = 0 to Array.length sorted_map - 1 do
				Array.sort (fun x y -> compare (fst cdf_data_points.(x).(i))
					(fst cdf_data_points.(y).(i))) sorted_map.(i)
				done;
			for i = 0 to var1_n - 1 do
				fprintf outfile "%12g" (i2f (i + 1) /. i2f var1_n);
				for j = 0 to !num_stats - 1 do
					let sorted_index = sorted_map.(j).(i) in
					let mean, conf_intvl = cdf_data_points.(sorted_index).(j) in
					fprintf outfile " %12g %12g" mean conf_intvl
					done;
				fprintf outfile "\n";
				done;
			flush outfile
			);
        fprintf outfile "\n\n";
        printf "\n"; flush stdout
        );
	close_out outfile;
    let name_lookup i =
    	assert(0 <= i && i <= List.length !stat_names);
        if i = 0 then (
			if plots = PlotCDF then
				"Fraction of " ^ var1name
			else
				var1name
			)
        else
			List.nth !stat_names (i-1)
        in
	(* Write, plot gnuplot files *)
	let plots_to_draw = match plots with
		PlotAll -> make_list (fun i -> (i+1, 0)) !num_stats
		| PlotSome these -> these
		| PlotCDF -> make_list (fun i -> (0, i+1)) !num_stats
		in
	let plots_to_show = ref [] in
    foreach plots_to_draw (fun (yaxis, xaxis) ->
    	let y_name = name_lookup yaxis in
        let x_name = name_lookup xaxis in
		let y_index = if yaxis = 0 then 1 else (yaxis - 1) * 2 + 2 in
		let x_index = if xaxis = 0 then 1 else (xaxis - 1) * 2 + 2 in
    	let base_plot_name = sprintf "%s-%s-vs-%s" base_outfile
		    (remove_special y_name) (remove_special x_name) in
		let gpl = open_out (base_plot_name ^ ".gpl") in
        fprintf gpl "set output \"%s.pdf\"
set terminal pdf
set xlabel \"%s\"
set ylabel \"%s\"
set grid
%s%s
plot [] [] \\\n"
			base_plot_name x_name y_name
			(if plots = PlotCDF then "set yrange [0:1]\n" else "")
			gnuplot_options;
		let plot_lines = ref [] in
		let add_plot_line line = plot_lines := line :: !plot_lines in
		foreach_i var2domain (fun j (var2valname, var2val) ->
			(* Color to plot: skip 6, which is typically an unreadable yellow *)
			let color = if j < 5 then j+1 else j+2 in
			let linetype = sprintf "lt %d lw 4 pt %d" color color in
			if confidence != None then
				(* Plot confidence intervals.  Note that we don't have conf.
				 * intervals for the var1 parameter, so depending on which
				 * (if any) axis the var1 goes, we have to use gnuplot's
				 * errorbars, xerrorbars, or xyerrorbars *)
				if y_index > 1 && x_index > 1 then
					add_plot_line (sprintf "\"\" index %d using %d:%d:%d:%d notitle w xyerrorbars %s"
						j x_index y_index (x_index+1) (y_index+1) linetype)
				else if y_index > 1 then
					add_plot_line (sprintf "\"\" index %d using %d:%d:%d notitle w errorbars %s"
						j x_index y_index (y_index+1) linetype)
				else if x_index > 1 then
					add_plot_line (sprintf "\"\" index %d using %d:%d:%d notitle w xerrorbars %s"
						j x_index y_index (x_index+1) linetype);
			(* Plot the main line *)
        	add_plot_line (sprintf "\"%s\" index %d using %d:%d title \"%s\" w lp %s"
            	(base_outfile ^ ".dat") j x_index y_index var2valname linetype)
            );
       	foreach (separate !plot_lines ", \\\n") (output_string gpl);
        fprintf gpl "\n";
        close_out gpl;
        ignore (Sys.command ("gnuplot \"" ^ base_plot_name ^ ".gpl\""));
		if show_plots then
			push plots_to_show (base_plot_name ^ ".pdf")
        );
	(* We're assuming Mac OS X here. The `joinpdfs' command takes a list of PDFs
	 * and combines them into one.  We do this only for easy presentation.
	 *)
	(* let joinpdfs = "/System/Library/Automator/Combine PDF Pages.action/Contents/Resources/join.py" in *)
	(* Actually, let's just use gs... *)
	let plots_to_show = map !plots_to_show (fun p -> sprintf "\"%s\"" p) in

	(* let cmd = sprintf "\"%s\" -o /tmp/joined.pdf %s" joinpdfs (print_list plots_to_show " ") in *)

	let cmd = "gs -dBATCH -dNOPAUSE -q -sDEVICE=pdfwrite -sOutputFile=/tmp/joined.pdf " ^ (print_list plots_to_show " ") in
	(* printf "cmd = \"%s\"\n" cmd; *)
	ignore (Sys.command cmd);
	if show_plots then
		ignore (Sys.command "open /tmp/joined.pdf")

let percentile p lst =
	let sorted_l = List.sort compare lst in
    List.nth sorted_l (int_of_float (p *. (i2f (List.length sorted_l - 1))))	

let mode lst =
	let h = Hashtbl.create (List.length lst) in
	let most_frequent_el = ref (List.hd lst) in
	let most_frequent_cnt = ref 1 in
	foreach lst (fun x ->
		let count = 1 + (try Hashtbl.find h x with Not_found -> 0) in
		Hashtbl.replace h x count;
		if count > !most_frequent_cnt then (
			most_frequent_cnt := count;
			most_frequent_el := x
			)
		);
	!most_frequent_el
	
let print_stats (s:(string*float) list) =
	foreach s (fun (name, v) ->
		printf "%25s %10g\n" name v)

(* Returns a list of the entries in the directory at `path'.
 * (To produce a valid pathname, `path' should be prepended
 * to each returned entry.) 
 *)
let get_dir_entries path =
  	let dir = Unix.opendir path in
	let rec get_dir_entries_aux entries =
    	try (
	    	let filename = Unix.readdir dir in
            get_dir_entries_aux (filename::entries)
            )
        with End_of_file ->
        	entries
		in
    get_dir_entries_aux []

(* Samples from the exponential distribution. *)
let exponential_dist mean () =
	(-. mean) *. log (Random.float 1.0)

(* Power-law (Pareto?) distribution with given mean.  Has probability density function
 *
 *     p(x) = a*b^a / x ^ alpha
 *
 * for  x >= b, where  a = alpha-1  and b = mean * (alpha-1) / (alpha-2),
 * and CDF
 *
 *     F(x) = 1 - (b/x)^a.
 *)
let powerlaw_dist alpha mean () =
	mean *. (alpha -. 2.) /. ((alpha -. 1.) *. ((Random.float 1.0) **
	(1. /. (alpha -. 1.))))


(* Translated power-law distribution, defined so  p(epsilon) > 0.  PDF is
 *
 *     p(x) = p1(x+b) = ab^a / (x+b)^(a+1),
 *
 * for x >= 0, where p1(x) is the standard power-law with the same exponent,
 * a = alpha-1, and  b = mean * (a-1), and CDF
 *
 *     F(x) = 1 - (b / (x+b))^a.
 *
 * Note: defined only for alpha > 2.
 *)
let translated_powerlaw_dist alpha mean _ =
   mean *. (alpha -. 2.) *. ((Random.float 1.0) ** (1. /. (1. -. alpha)) -. 1.)

(* Format expected is one sample per line, in floating-point, possibly with
 * extra junk on the line after the sample (but separated by a space). *)
let dist_of_file fname =
    let infile = open_in fname in
    let rec read_file_aux data =
        let sample =
            try (
				Some (float_of_string(List.hd (Str.split
					(Str.regexp " ") (input_line infile)))))
            with End_of_file -> None
            in
        match sample with
            None -> data
            | Some p ->
                read_file_aux (p::data)
        in
    let data = read_file_aux [] in
    close_in infile;
    let n = List.length data in
	let samples = Array.of_list data in
    fun () -> samples.(Random.int n)

(* Given a set of samples <data>, output in <file_path> a CDF of the data
 * in gnuplot format.
 *)
let output_cdf data comparator datum_to_string out_stream =
	let n = List.length data in
	let fl_n = i2f n in
	let data = List.sort comparator data in
	let i = ref 0 in
	let last = ref (List.hd data) in
	
	let print_point x y = fprintf out_stream "%s %g\n" (datum_to_string x) y in

	foreach data (fun d ->
		if d <> !last then (
			print_point !last ((i2f !i) /. fl_n);
			print_point d     ((i2f !i) /. fl_n);
			last := d
			);
		incr i;
		if !i = n then
			print_point d 1.0
		)

let output_cdf_floats data file_path =
	let out_stream = open_out file_path in
	output_cdf data compare (fun x -> sprintf "%g" x) out_stream;
	close_out out_stream

let output_cdf_ints data file_path =
	let out_stream = open_out file_path in
	output_cdf data compare string_of_int out_stream;
	close_out out_stream

class freq_counter = object(self)
	val samples = Hashtbl.create 1
	val mutable n = Int64.zero
	
	method add_samples num_samples sample_value =
		let freq = hashtbl_find_default samples sample_value 0L in
		Hashtbl.replace samples sample_value (Int64.add freq num_samples);
		n <- Int64.add n num_samples

	method output_cdf file_path =
		let vals = List.sort compare (hashtbl_keys samples) in
		let file = open_out file_path in
		let m = ref Int64.zero in
		foreach vals (fun v ->
			m := Int64.add !m (Hashtbl.find samples v);
			fprintf file "%d %f %Ld\n" v (Int64.to_float !m /. Int64.to_float n) !m
			);
		close_out file
	end

(* A new version of the above frequency counter, based on a struct,
 * and with parameterized data type. *)
module FreqCounter = struct
	type 'a t = {
		samples   : ('a, Int64.t) Hashtbl.t;
		mutable n : Int64.t;
		}
	
	let create () = {
		samples = Hashtbl.create 1;
		n = Int64.zero;
		}
	
	let add_samples this num_samples sample_value =
		let freq = hashtbl_find_default this.samples sample_value 0L in
		Hashtbl.replace this.samples sample_value (Int64.add freq num_samples);
		this.n <- Int64.add this.n num_samples

	let add_sample this sample_value =
		add_samples this 1L sample_value

	let output_cdf this sample_to_string file_path =
		let vals = List.sort compare (hashtbl_keys this.samples) in
		let file = open_out file_path in
		let m = ref Int64.zero in
		foreach vals (fun v ->
			m := Int64.add !m (Hashtbl.find this.samples v);
			fprintf file "%s %f %Ld\n" (sample_to_string v) (Int64.to_float !m /. Int64.to_float this.n) !m
			);
		close_out file
	
	let output_samples this sample_to_string file_path =
		let file = open_out file_path in
		foreach_hashtbl this.samples (fun sample count ->
			let i = ref 0L in
			while !i < count do
				fprintf file "%s\n" (sample_to_string sample);
				i := Int64.add !i 1L
				done
			);
		close_out file
	
	(* Note, this is a quick implementation that sorts & scans the whole hashtable. *)
	let percentile this p =
		let point = Int64.of_float (p *. (Int64.to_float this.n)) in
		let vals = ref (List.sort compare (hashtbl_keys this.samples)) in
		let m = ref 0L in
		while point > Int64.add !m (Hashtbl.find this.samples (List.hd !vals)) do
			m := Int64.add !m (Hashtbl.find this.samples (pop vals))
			done;
		List.hd !vals

	end








(* Computes correlation coefficient of two arrays (for a definition see e.g.
 * http://mathworld.wolfram.com/CorrelationCoefficient.html).
 *)
let correlation_coefficient x y =
	let n = Array.length x in
	let avg a = (Array.fold_right (+.) a 0.0) /. i2f n in
	let ss a a_avg b b_avg =
		let s = ref 0.0 in
		for i = 0 to n-1 do
			s := !s +. (a.(i) -. a_avg) *. (b.(i) -. b_avg)
			done;
		!s
		in
	let xavg = avg x in
	let yavg = avg y in
	let ssxx = ss x xavg x xavg in
	let ssyy = ss y yavg y yavg in
	let ssxy = ss x xavg y yavg in
	sqrt (ssxy *. ssxy /. (ssxx *. ssyy))

(* DEPRECATED -- use TimeAverager module below.  THough, note that one is int64... *)
class time_avg = object(self)
    val mutable start_time = 0.0
	val mutable last_change_time = 0.0
	val mutable sum = 0.0
	val mutable current_value = 0.0

	method reset _start_time =
		start_time       <- _start_time;
		last_change_time <- 0.0;
		sum              <- 0.0;
		current_value    <- 0.0

	method update t v =
		assert(t >= last_change_time);
		sum <- sum +. (t -. last_change_time) *. current_value;
		last_change_time <- t;
		current_value <- v

	method mean t =
		assert(t >= last_change_time);
		self#update t current_value;
		sum /. (t -. start_time)
	end

module TimeAverager = struct
	type t = {
    	mutable start_time       : Int64.t;
		mutable last_change_time : Int64.t;
		mutable sum              : float;
		mutable current_value    : float;
		}

	let create start_time = {
		start_time       = start_time;
		last_change_time = 0L;
		sum              = 0.0;
		current_value    = 0.0;
		}

	let reset ta start_time =
		ta.start_time       <- start_time;
		ta.last_change_time <- 0L;
		ta.sum              <- 0.0;
		ta.current_value    <- 0.0

	let update ta t v =
		assert(t >= ta.last_change_time);
		ta.sum <- ta.sum +. (Int64.to_float (Int64.sub t ta.last_change_time)) *. ta.current_value;
		ta.last_change_time <- t;
		ta.current_value <- v

	let mean ta t =
		assert(t >= ta.last_change_time);
		update ta t ta.current_value;
		ta.sum /. Int64.to_float (Int64.sub t ta.start_time)
	end

(************************************************************************
 * Command-running
 ************************************************************************)

let run_jobs
	?(procs_per_node:int=1)
	?(randomize:bool=true)
	nodes commands out_dir
	=
	let num_procs = procs_per_node * Array.length nodes in
	(* Assign tasks to processors *)
	let ordered_tasks =
		if randomize then
			permute_arr (Array.of_list commands)
		else
			Array.of_list commands
		in
	let tasks = Array.make num_procs [] in
	for i = 0 to Array.length ordered_tasks - 1 do
		let j = i mod num_procs in
		tasks.(j) <- ordered_tasks.(i) :: tasks.(j)
		done;
	(* Clear out old process logs which can cause confusion *)
	let exit_code = Sys.command(sprintf "cd %s; rm -f proc*.log" out_dir) in
	assert(exit_code = 0);
	(* Run! *)
	for i = 0 to num_procs - 1 do
		printf "Starting process %d...\n" i; flush stdout;
		if Unix.fork() == 0 then (
			let node = nodes.(i mod Array.length nodes) in
			let log = open_out (sprintf "%s/proc%d.log" out_dir i) in
			fprintf log "Process %d: local pid %d, remote node %s, %d tasks\n" i (Unix.getpid())
				node (List.length tasks.(i)); flush log;
			foreach_i tasks.(i) (fun j cmd ->
				fprintf log "Task %d of %d: running \"%s\"..." (j+1)
					(List.length tasks.(i)) cmd; flush log;
				let full_cmd =
					if node = "localhost" then
						cmd
					else
						(* Old gexec version 
						sprintf "export GEXEC_SVRS=\"%s\"; gexec -n 1 -p none sh -c \"%s\""
							node cmd
						*)
						(* New ssh version *)
						sprintf "ssh %s \"%s\"" node cmd
					in
				let t1 = Unix.time () in
				let exit_status = Sys.command full_cmd in
				let t2 = Unix.time () in
				let status_msg =
					match exit_status with
						0 -> ""
						| x -> (
							let msg = sprintf "(ERROR: exit code %d on command \"%s\")" x full_cmd in
							fprintf stderr "%s\n" msg;
							msg
							)
					in
				fprintf log "done in %f minutes %s\n" ((t2 -. t1) /. 60.) status_msg
				);
			fprintf log "Process %d done\n" i; flush log;
			close_out log;
			exit(0)
			);
		done
