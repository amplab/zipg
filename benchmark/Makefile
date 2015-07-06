CC := g++

SRCDIR := src
BUILDDIR := ../build
BINDIR := ../bin
LIBDIR := ../external/succinct-cpp/lib
NEO4JDIR := neo4j

OBJ := $(shell find $(BUILDDIR) -type f -name *Graph*.o)
TARGET_S := $(BINDIR)/bench
OBJ_S := $(BUILDDIR)/succinct-bench.o
TARGET_C := $(BINDIR)/create
OBJ_C := $(BUILDDIR)/succinct-create.o

CFLAGS := -O3 -std=c++11 -Wall -g
LIB := -lpthread -L $(LIBDIR) -lsuccinct
INC := -I ../include -I include -I ../external/succinct-cpp/benchmark/include/

all: create bench

create: $(TARGET_C)

bench: $(TARGET_S)

$(TARGET_C): $(OBJ_C) $(OBJ)
	@echo "Linking create file..."
	@mkdir -p $(BINDIR)
	@echo " $(CC) $^ -o $(TARGET_C) $(LIB)"; $(CC) $^ -o $(TARGET_C) $(LIB)

$(TARGET_S): $(OBJ_S) $(OBJ)
	@echo "Linking bench file..."
	@mkdir -p $(BINDIR)
	@echo " $(CC) $^ -o $(TARGET_S) $(LIB)"; $(CC) $^ -o $(TARGET_S) $(LIB)

$(BUILDDIR)/%.o: $(SRCDIR)/%.cpp
	@mkdir -p $(BUILDDIR)
	@echo " $(CC) $(CFLAGS) $(INC) -c -o $@ $<"; $(CC) $(CFLAGS) $(INC) -c -o $@ $<

clean:
	@echo "Cleaning...";
	@echo " $(RM) -r $(BUILDDIR) $(TARGET_S)";\
		$(RM) $(OBJ_S) $(TARGET_S)

.PHONY: clean
