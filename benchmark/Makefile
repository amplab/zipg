CC := g++

SRCDIR := src
BUILDDIR := ../build
BINDIR := ../bin
LIBDIR := ../external/succinct-cpp/lib

TARGET_S := $(BINDIR)/bench
OBJ_S := $(BUILDDIR)/succinct-bench.o
TARGET_C := $(BINDIR)/create_nodes
OBJ_C := $(BUILDDIR)/succinct-create.o

CFLAGS := -O3 -std=c++11 -Wall -g
LIB := -L $(LIBDIR) -lsuccinct -L ../lib -lsuccinctgraph
INC := -I ../include -I include -I ../external/succinct-cpp/benchmark/include/

all: create bench

create: $(TARGET_C)

bench: clean $(TARGET_S)

$(TARGET_C): $(OBJ_C)
	@echo "Linking create file..."
	@mkdir -p $(BINDIR)
	@echo " $(CC) $^ -o $(TARGET_C) $(LIB)"; $(CC) $^ -o $(TARGET_C) $(LIB)

$(TARGET_S): $(OBJ_S)
	@echo "Linking bench file..."
	@mkdir -p $(BINDIR)
	@echo " $(CC) $^ -o $(TARGET_S) $(LIB)"; $(CC) $^ -o $(TARGET_S) $(LIB)

$(BUILDDIR)/%.o: $(SRCDIR)/%.cpp
	@mkdir -p $(BUILDDIR)
	@echo " $(CC) $(CFLAGS) $(INC) -c -o $@ $<"; $(CC) $(CFLAGS) $(INC) -c -o $@ $<

clean:
	@echo "Cleaning...";
	@echo " $(RM) -r $(BUILDDIR) $(TARGET_S)";\
		$(RM) -r $(BUILDDIR) $(TARGET_S)
	
.PHONY: clean
