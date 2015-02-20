CC := g++

SRCDIR := ../benchmark
BUILDDIR := ../build/bench
BINDIR := ../bin

TARGET_C := $(BINDIR)/commands
OBJ_C := $(BUILDDIR)/succinct-commands.o
TARGET_S := $(BINDIR)/bench
OBJ_S := $(BUILDDIR)/succinct-bench.o

CFLAGS := -O3 -std=c++11 -Wall -g
LIB := -L ../external/succinct-cpp/lib -lsuccinct
INC := -I ../include

all: bench commands

bench: $(TARGET_S)

commands: $(TARGET_C)

$(TARGET_C): $(OBJ_C)
	@echo "Linking..."
	@mkdir -p $(BINDIR)
	@echo " $(CC) $^ -o $(TARGET_C) $(LIB)"; $(CC) $^ -o $(TARGET_C) $(LIB)

$(TARGET_S): $(OBJ_S)
	@echo "Linking..."
	@mkdir -p $(BINDIR)
	@echo " $(CC) $^ -o $(TARGET_S) $(LIB)"; $(CC) $^ -o $(TARGET_S) $(LIB)
	
$(BUILDDIR)/%.o: $(SRCDIR)/%.cpp
	@mkdir -p $(BUILDDIR)
	@echo " $(CC) $(CFLAGS) $(INC) -c -o $@ $<"; $(CC) $(CFLAGS) $(INC) -c -o $@ $<

clean:
	@echo "Cleaning...";
	@echo " $(RM) -r $(BUILDDIR) $(TARGET)"; $(RM) -r $(BUILDDIR) $(TARGET)
	
.PHONY: clean
