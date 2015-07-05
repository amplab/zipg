CC := g++
AR := ar

BINDIR := bin
BUILDDIR := build
EXTERNDIR := external
LIBDIR := lib
INCLUDEDIR := include
SRCDIR := src
SUCCINCTDIR := $(EXTERNDIR)/succinct-cpp
THRIFT_BIN := $(SUCCINCTDIR)/bin/thrift

SRCDIRS := $(shell find $(SRCDIR) -type d)
BUILDDIRS := $(subst $(SRCDIR),$(BUILDDIR),$(SRCDIRS))

SOURCES := $(shell find $(SRCDIR) -type f -name *.cpp)
OBJECTS := $(patsubst $(SRCDIR)/%,$(BUILDDIR)/%,$(SOURCES:.cpp=.o))
TARGET := $(LIBDIR)/libsuccinctgraph.a

CFLAGS := -O3 -std=c++11 -Wall -g
LIB :=  -lpthread -L ../external/succinct-cpp/lib -lsuccinct -lthrift
INC := -I include

all: succinct graph

sharding: $(THRIFT_BIN)
	@mkdir -p src/thrift
	@mkdir -p include/thrift
	$(THRIFT_BIN) -I include/thrift \
	  -gen cpp:include_prefix \
	  -out thrift \
	  thrift/succinct_graph.thrift
	mv thrift/*.cpp src/thrift/ && mv thrift/*.h include/thrift/
	rm -rf src/thrift/*skeleton*

$(THRIFT_BIN):
	@cd $(SUCCINCTDIR) && make gen-thrift

succinct-server: graph $(THRIFTTARGET_SS)

succinct-handler: graph $(THRIFTTARGET_SH)

succinct-master: graph $(THRIFTTARGET_SM)

$(THRIFTTARGET_SS): $(THRIFTOBJECTS_SS) $(THRIFTOBJECTS_GEN)
	@echo "Linking..."
	@mkdir -p $(BINDIR)
	$(CC) $^ -o $(THRIFTTARGET_SS) $(THRIFTLIB)


build-thrift:
	cd external/succinct-cpp/ && make -j build-thrift

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

$(BUILDDIR)/%.o: $(SRCDIR)/%.cpp
	@mkdir -p $(BUILDDIRS)
	@echo " $(CC) $(CFLAGS) $(SGFLAGS) $(INC) -c -o $@ $<";\
	        $(CC) $(CFLAGS) $(SGFLAGS) $(INC) -c -o $@ $<

bench: graph
	cd benchmark && $(MAKE)

clean:
	echo "Cleaning...";
	cd $(SUCCINCTDIR) && $(MAKE) clean
	cd external/graphs && $(MAKE) clean
	rm -rf $(BINDIR)/*  $(BUILDDIR)/* $(LIBDIR)/*

