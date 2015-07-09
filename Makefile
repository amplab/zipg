CC := g++
AR := ar

BINDIR := bin
BUILDDIR := build
EXTERNDIR := external
LIBDIR := lib
INCLUDEDIR := include
SRCDIR := src/succinct-graph
SUCCINCTDIR := $(EXTERNDIR)/succinct-cpp

SRCDIRS := $(shell find $(SRCDIR) -type d)
BUILDDIRS := $(subst $(SRCDIR),$(BUILDDIR),$(SRCDIRS)) build/thrift

SOURCES := $(shell find $(SRCDIR) -type f -name *.cpp)
OBJECTS := $(patsubst $(SRCDIR)/%,$(BUILDDIR)/%,$(SOURCES:.cpp=.o))
TARGET := $(LIBDIR)/libsuccinctgraph.a
TARGET_GRAPH_CLIENT := $(LIBDIR)/libsuccinctgraph-client.a

CFLAGS := -O3 -std=c++11 -Wall -g
LIB :=  -lpthread -L ../external/succinct-cpp/lib -lsuccinct
INC := -I include

# Function used to check variables. Use on the command line:
# make print-VARNAME
# Useful for debugging and adding features
print-%: ; @echo $*=$($*)

############################# Thrift / RPC related #############################

THRIFT_BIN := $(SUCCINCTDIR)/bin/thrift
THRIFTCFLAGS := -O3 -std=c++11 -w -DHAVE_NETINET_IN_H -g
THRIFTLIB := -L $(LIBDIR) -L external/succinct-cpp/lib -levent -lthrift -lsuccinct -lsuccinctgraph
# the latter contains Thrift itself
THRIFTINC := -I include -I $(SUCCINCTDIR)/include

# non-generated RPC related code
rpc_src_dir := src/rpc
rpc_build_dir := build/rpc

# graph query shard
RPC_TARGET_GRAPH_QUERY_SERVER := $(BINDIR)/graph_query_server
RPC_SRC_GRAPH_QUERY_SERVER := $(rpc_src_dir)/GraphQueryServiceServer.cpp
RPC_OBJECTS_GRAPH_QUERY_SERVER := $(rpc_build_dir)/GraphQueryServiceServer.o

# graph query aggregator
RPC_TARGET_GRAPH_QUERY_AGGREGATOR := $(BINDIR)/graph_query_aggregator
RPC_SRC_GRAPH_QUERY_AGGREGATOR := $(rpc_src_dir)/GraphQueryAggregator.cpp
RPC_OBJECTS_GRAPH_QUERY_AGGREGATOR := $(rpc_build_dir)/GraphQueryAggregator.o

# auto-generated thrift files for the services
# Reason for this manually-entered atrocity: lazy eval / thunk issue
THRIFTSRCDIR := src/thrift
THRIFTBUILDDIR := build/thrift
THRIFT_GENERATED_SOURCES := $(THRIFTSRCDIR)/GraphQueryService.cpp $(THRIFTSRCDIR)/succinct_graph_constants.cpp $(THRIFTSRCDIR)/succinct_graph_types.cpp $(THRIFTSRCDIR)/GraphQueryAggregatorService.cpp
THRIFT_GENERATED_OBJECTS := $(patsubst $(THRIFTSRCDIR)/%,$(THRIFTBUILDDIR)/%,$(THRIFT_GENERATED_SOURCES:.cpp=.o))

target_graph_partitioner := $(BINDIR)/graph-partitioner
obj_graph_partitioner := $(BUILDDIR)/partitioners.o
src_graph_partitioner := $(SRCDIR)/partitioners.cpp

# 1st target is the default
all: succinct graph

graph-partitioner: $(target_graph_partitioner)
$(target_graph_partitioner): $(obj_graph_partitioner)
	@mkdir -p $(BINDIR)
	$(CC) $^ -o $@
$(obj_graph_partitioner): $(src_graph_partitioner)
	@mkdir -p $(BUILDDIR)
	$(CC) $(CFLAGS) $(INC) -c -o $@ $<

$(THRIFT_BIN):
	cd $(SUCCINCTDIR) && make -j build-thrift

generate_thrift_manifest = $(BUILDDIR)/generate_thrift.manifest

$(generate_thrift_manifest): $(THRIFT_BIN) thrift/succinct_graph.thrift
	@mkdir -p src/thrift
	@mkdir -p include/thrift
	$(THRIFT_BIN) -I include/thrift \
	  -gen cpp:include_prefix \
	  -out thrift \
	  thrift/succinct_graph.thrift
	mv thrift/*.cpp src/thrift/ && mv thrift/*.h include/thrift/
	rm -rf src/thrift/*skeleton*
	touch $(generate_thrift_manifest)

$(rpc_build_dir)/%.o: $(rpc_src_dir)/%.cpp $(generate_thrift_manifest)
	@echo "making rpc stuff"
	@mkdir -p $(rpc_build_dir)
	$(CC) $(CFLAGS) $(THRIFTINC) -c -o $@ $<

$(THRIFTBUILDDIR)/%.o: $(THRIFTSRCDIR)/%.cpp $(generate_thrift_manifest)
	@echo "making thrift stuff"
	@mkdir -p $(THRIFTBUILDDIR)
	$(CC) $(THRIFTCFLAGS) $(THRIFTINC) -c -o $@ $<

$(RPC_TARGET_GRAPH_QUERY_SERVER): $(RPC_OBJECTS_GRAPH_QUERY_SERVER) $(THRIFT_GENERATED_OBJECTS)
	@echo "Linking..."
	@mkdir -p $(BINDIR)
	$(CC) $^ -o $(RPC_TARGET_GRAPH_QUERY_SERVER) $(THRIFTLIB)

$(RPC_TARGET_GRAPH_QUERY_AGGREGATOR): $(RPC_OBJECTS_GRAPH_QUERY_AGGREGATOR) $(THRIFT_GENERATED_OBJECTS)
	@echo "Linking..."
	@mkdir -p $(BINDIR)
	$(CC) $^ -o $(RPC_TARGET_GRAPH_QUERY_AGGREGATOR) $(THRIFTLIB)

rpc: succinct graph $(RPC_TARGET_GRAPH_QUERY_SERVER) $(RPC_TARGET_GRAPH_QUERY_AGGREGATOR)
graph-server: succinct graph $(RPC_TARGET_GRAPH_QUERY_SERVER)
graph-aggregator: succinct graph $(RPC_TARGET_GRAPH_QUERY_AGGREGATOR)

succinct:
	mkdir -p $(LIBDIR)
	git submodule sync # make sure to fetch new updates
	git submodule update
	cd $(SUCCINCTDIR) && $(MAKE)

graph_generator:
	cd external/graphs && $(MAKE)
	mv external/graphs/generate_graphs $(BINDIR)

graph: $(TARGET)

$(TARGET): $(OBJECTS)
	@echo "Creating static library..."
	@mkdir -p $(LIBDIR)
	@echo " $(AR) $(ARFLAGS) $@ $^"; $(AR) $(ARFLAGS) $@ $^

graph-client: $(TARGET_GRAPH_CLIENT)

$(TARGET_GRAPH_CLIENT): $(THRIFT_GENERATED_OBJECTS)
	@echo "Creating static library..."
	@mkdir -p $(LIBDIR)
	@echo " $(AR) $(ARFLAGS) $@ $^"; $(AR) $(ARFLAGS) $@ $^

$(BUILDDIR)/%.o: $(SRCDIR)/%.cpp
	@mkdir -p $(BUILDDIRS)
	@echo " $(CC) $(CFLAGS) $(SGFLAGS) $(INC) -c -o $@ $<";\
	        $(CC) $(CFLAGS) $(SGFLAGS) $(INC) -c -o $@ $<

bench: graph
	cd benchmark && $(MAKE)

sharded-bench: rpc graph-client
	cd benchmark && $(MAKE) sharded-bench

clean:
	echo "Cleaning...";
	cd $(SUCCINCTDIR) && $(MAKE) clean
	cd external/graphs && $(MAKE) clean
	rm -rf $(BINDIR)/*  $(BUILDDIR)/* $(LIBDIR)/*
	rm -rf include/thrift/* src/thrift/*
