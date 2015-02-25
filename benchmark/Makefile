CC := g++

SRCDIR := ../benchmark
BUILDDIR := ../build
BINDIR := ../bin

TARGET_S := $(BINDIR)/bench
OBJ_S := $(BUILDDIR)/succinct-bench.o

CFLAGS := -O3 -std=c++11 -Wall -g
LIB := -L ../external/succinct-cpp/lib -lsuccinct -L ../lib -lsuccinctgraph
INC := -I ../include

all: bench

bench: $(TARGET_S)

$(TARGET_S): $(OBJ_S)
	@echo "Linking..."
	@mkdir -p $(BINDIR)
	@echo " $(CC) $^ -o $(TARGET_S) $(LIB)"; $(CC) $^ -o $(TARGET_S) $(LIB)
	
$(BUILDDIR)/%.o: $(SRCDIR)/%.cpp
	@mkdir -p $(BUILDDIR)
	@echo " $(CC) $(CFLAGS) $(INC) -c -o $@ $<"; $(CC) $(CFLAGS) $(INC) -c -o $@ $<

clean:
	@echo "Cleaning...";
	@echo " $(RM) -r $(BUILDDIR) $(TARGET_S)"; $(RM) -r $(BUILDDIR) $(TARGET_S)
	
.PHONY: clean
