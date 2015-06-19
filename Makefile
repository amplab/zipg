CC := g++
AR := ar

BINDIR := bin
BUILDDIR := build
EXTERNDIR := external
LIBDIR := lib
INCLUDEDIR := include
SRCDIR := src
SUCCINCTDIR := $(EXTERNDIR)/succinct-cpp

SRCDIRS := $(shell find $(SRCDIR) -type d)
BUILDDIRS := $(subst $(SRCDIR),$(BUILDDIR),$(SRCDIRS))

SOURCES := $(shell find $(SRCDIR) -type f -name *.cpp)
OBJECTS := $(patsubst $(SRCDIR)/%,$(BUILDDIR)/%,$(SOURCES:.cpp=.o))
TARGET := $(LIBDIR)/libsuccinctgraph.a
CFLAGS := -O3 -std=c++11 -Wall -g
LIB := -L ../external/succinct-cpp/lib -lsuccinct
INC := -I include

all: succinct graph

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

