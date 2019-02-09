TARGET = test
LDFLAGS_D		:= -L.. -L. -Wl,-rpath=. -Wl,-rpath=..
LDLIBS_D		:= -lcriterion -lrydb
LDFLAGS_S		:= ../librydb.a
LDLIBS_S		:= -lcriterion

LDFLAGS = $(LDFLAGS_D)
LDLIBS = $(LDLIBS_D)

RYDB_MAKE_RULE = all

CC = clang
O=0
CCACHE = ccache
COLOR = always
COLOR_OPT = -fdiagnostics-color=${COLOR}
CFLAGS = -I../ -ggdb -O${O} -Wall -Wextra $(COLOR_OPT) -Wpointer-sign -Wpointer-arith -Wshadow  -Wnested-externs -Wsign-compare -Wpedantic
VALGRIND_FLAGS = --tool=memcheck --track-origins=yes --read-var-info=yes --leak-check=full --show-leak-kinds=all --leak-check-heuristics=all --keep-stacktraces=alloc-and-free
CALLGRIND_FLAGS = --tool=callgrind --collect-jumps=yes  --collect-systime=yes --branch-sim=yes --cache-sim=yes --simulate-hwpref=yes --simulate-wb=yes --callgrind-out-file=callgrind-rydb-%p.out

.PHONY: default all clean force run 

default: $(TARGET)
all: default

OBJECTS = $(patsubst %.c, %.o, $(wildcard *.c))
HEADERS = $(wildcard *.h)

$(OBJECTS): .compiler_flags

%.o: %.c $(HEADERS)
	$(CCACHE) $(CC) $(CFLAGS) -c $< -o $@

.PRECIOUS: $(TARGET) $(OBJECTS)

$(TARGET): rydb
$(TARGET): $(OBJECTS)
	$(CCACHE) $(CC) $(OBJECTS) $(LDFLAGS) $(LDLIBS) -o $@

rydb:
	$(MAKE) -C ../ $(RYDB_MAKE_RULE)

clean:
	-rm -f *.o
	-rm -f $(TARGET)

.compiler_flags: force
	@echo '$(CC) $(CFLAGS) $(LDFLAGS) $(LDLIBS)' | cmp -s - $@ || echo '$(CC) $(CFLAGS) $(LDFLAGS) $(LDLIBS)' > $@

dynamic: LDFLAGS = $(LDFLAGS_D)
dynamic: LDLIBS  = $(LDLIBS_D)
dynamic: default

static: LDFLAGS = $(LDFLAGS_S)
static: LDLIBS  = $(LDLIBS_S)
static: default
	

run: default
	./$(TARGET)

debug: default
	./$(TARGET) --debug=gdb &
	sudo kdbg -r localhost:1234 ./$(TARGET)

valgrind: gcc5
	valgrind $(VALGRIND_FLAGS)  ./$(TARGET)

callgrind: default
	valgrind $(CALLGRIND_FLAGS)  ./$(TARGET)

coverage: CFLAGS += -fprofile-arcs -ftest-coverage
coverage: LDFLAGS += -fprofile-arcs -ftest-coverage
coverage: RYDB_MAKE_RULE = coverage
coverage: default
	
