CC := g++

BINDIR := bin
EXTERNDIR := external
LIBDIR := lib
INCLUDEDIR := include
SUCCINCTDIR := $(EXTERNDIR)/succinct-cpp

all: succinct

succinct:
	mkdir -p $(LIBDIR)
	cd $(SUCCINCTDIR) && $(MAKE)

clean:
	echo "Cleaning...";
	cd $(SUCCINCTDIR) && $(MAKE) clean
	rm -rf $(BINDIR)/* $(LIBDIR)/* $(INCLUDEDIR)/succinct
