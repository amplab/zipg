CC := g++

SRCDIR := src
BUILDDIR := ../build
BINDIR := ../bin
LIBDIR := ../external/succinct-cpp/lib
NEO4JDIR := neo4j

OBJ := $(shell find $(BUILDDIR) -type f -name Succinct*.o) $(BUILDDIR)/GraphFormatter.o
TARGET_S := $(BINDIR)/bench
OBJ_S := $(BUILDDIR)/succinct-bench.o
TARGET_C := $(BINDIR)/create
OBJ_C := $(BUILDDIR)/succinct-create.o

CFLAGS := -O3 -std=c++11 -Wall -g
LIB := -lpthread -L $(LIBDIR) -lsuccinct
# What atrocity am I committing
INC := -I ../include -I include -I ../external/succinct-cpp/benchmark/include/ -I ../external/succinct-cpp/include
# rpclib := -L $(LIBDIR) -L ../lib -levent -lthrift -lsuccinct -lsuccinctgraph -lsuccinctgraph-client

rpclib :=

UNAME_S := $(shell uname -s)
# FIXME: super hacky
ifeq ($(UNAME_S),Linux)
	# Linux, g++
	# NB: -l order matters (-lsuccinct must come after -lsuccinctgraph); also on
	# EC2 if not statically link thrift, the binaries seem unable to find the
	# library file (even though LD_LIBRARY_PATH is set correctly)
	rpclib += -lpthread -L $(LIBDIR) -L ../lib \
	  -Wl,-Bdynamic -lsuccinctgraph-client -lsuccinctgraph -lsuccinct \
	  -Wl,-Bstatic -lthrift \
	  -Wl,-Bdynamic -Wl,--as-needed
endif
ifeq ($(UNAME_S),Darwin)
	# Mac OS, clang
	rpclib += -lpthread -L $(LIBDIR) -L ../lib -levent -lthrift -lsuccinct -lsuccinctgraph -lsuccinctgraph-client
endif


all: create bench

create: $(TARGET_C)

bench: $(TARGET_S)

$(TARGET_C): $(OBJ_C) $(OBJ)
	@echo "Linking create file..."
	@mkdir -p $(BINDIR)
	$(CC) $^ -o $(TARGET_C) $(rpclib)

$(TARGET_S): $(OBJ_S) $(OBJ)
	@echo "Linking bench file..."
	@mkdir -p $(BINDIR)
	$(CC) $^ -o $(TARGET_S) $(rpclib)

$(BUILDDIR)/%.o: $(SRCDIR)/%.cpp
	@mkdir -p $(BUILDDIR)
	$(CC) $(CFLAGS) $(SGFLAGS) $(INC) -c -o $@ $<

clean:
	@echo "Cleaning...";
	@echo " $(RM) -r $(BUILDDIR) $(TARGET_S)";\
		$(RM) $(OBJ_S) $(TARGET_S)

.PHONY: clean
