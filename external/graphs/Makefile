OCAMLC=ocamlopt.opt
#OCAMLC=ocamlc -g
#-I $(INCLUDES) 
# Important: order is important, i.e. 1st module shouldn't depend on any other.
MODULES=unix.cmxa str.cmxa
#MODULES=unix.cma str.cma
BINARIES=generate_graphs

all: $(BINARIES)
	$(OCAMLC) -c util.ml

#sim: util.ml pqueue.ml simulator.ml graph.ml netsim.ml pathvector.ml compact.ml sim.ml
#	$(OCAMLC) -o sim $(MODULES) util.ml pqueue.ml simulator.ml \
#		graph.ml netsim.ml pathvector.ml compact.ml sim.ml

generate_graphs: util.ml graph.ml generate_graphs.ml
	$(OCAMLC) -o generate_graphs $(MODULES) util.ml pqueue.ml graph.ml \
		generate_graphs.ml

clean:
	rm -f *.cmx *.cmi *.cmo *.o $(BINARIES)
