open Printf
open Util

let largest_connected_subgraph g =
    let components = Graph.connected_components g in
    let biggest = argmax components List.length in
    Graph.subgraph g biggest

let main =
    Random.self_init ();

    let n = (int_of_string) Sys.argv.(1) in
    let m = (int_of_string) Sys.argv.(2) in
    let g = Graph.generate_gnm n m in
    (* format is "src dst " *)
    Graph.write_to_file g (fun () -> "") (sprintf "%d.edge" n)

    (* [16; 32; 64; 128; 256; 512; 1024; 2048; 4096; 8192; 16384; 32768;] *)
    (*
    foreach [768; 1280; 1536; 1792] (fun n ->
        foreach [2; 3; 4; 5; 6] (fun d ->
            for i = 1 to 10 do
                let g = Graph.generate_gnm n (d*n) in
                let g = largest_connected_subgraph g in
                Graph.write_to_file g (fun () -> "")
                    (sprintf "/tmp/topologies/gnm_%d_%d_%d.txt" n (2*d) i)
                done
            )
        )
    *)

    (*
    foreach [16; 32; 64; 128; 256; 512; 1024; 2048; 4096; 8192; 16384; 32768;] (fun n ->
        foreach [4;6;8;10;12] (fun d ->
            for i = 1 to 10 do
                let g = Graph.generate_geometric_random_graph n (i2f d) in
                let g = largest_connected_subgraph g in
                Graph.write_to_file g string_of_float
                    (sprintf "/tmp/geometric_random_graphs/geometric_rg_%d_%d_%d.txt" n d i)
                done
            )
        )
    *)

    (*
    let g = Graph.generate_geometric_random_graph 1000 8. in
    let g = largest_connected_subgraph g in
    Graph.write_to_file g (fun _ -> "") "/tmp/graph.txt";
    Graph.visualize_geometric_graph g
    *)
