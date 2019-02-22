TARGET = test
LDFLAGS_D	:= -L.. -L. -Wl,-rpath=. -Wl,-rpath=..
LDLIBS_D	:= -lrydb -rdynamic
LDFLAGS_S	:= ../librydb.a
LDLIBS_S	:= 

LDFLAGS = $(LDFLAGS_D)
LDLIBS = $(LDLIBS_D)

CC = clang
O=0
CCACHE = ccache
COLOR = always
TIMESTAMP = `date +'%y-%m-%d-%H.%M.%S'`
COLOR_OPT = -fdiagnostics-color=${COLOR}
CFLAGS = -DSNOW_ENABLED -I../ -ggdb -O${O} -Wall -Wextra $(COLOR_OPT) -Wpointer-sign -Wpointer-arith -Wshadow  -Wnested-externs -Wsign-compare -Wpedantic  -DRYDB_DEBUG
VALGRIND_FLAGS = --trace-children=no --tool=memcheck --track-origins=yes --read-var-info=yes --leak-check=full --show-leak-kinds=all --leak-check-heuristics=all --keep-stacktraces=alloc-and-free
CALLGRIND_OUTFILE = callgrind-rydb-$(TIMESTAMP).out
CALLGRIND_FLAGS = --tool=callgrind --dump-instr=yes --collect-jumps=yes  --collect-systime=yes --branch-sim=yes --cache-sim=yes --simulate-hwpref=yes --simulate-wb=yes --callgrind-out-file=$(CALLGRIND_OUTFILE)

TEST = *

.PHONY: default all clean force run coverage coverage-gcc

default: $(TARGET)
all: default

OBJECTS = $(patsubst %.c, %.o, $(wildcard *.c))
HEADERS = $(wildcard *.h)

$(OBJECTS): .compiler_flags

%.o: %.c $(HEADERS)
	$(CCACHE) $(CC) $(CFLAGS) -c $< -o $@

.PRECIOUS: $(TARGET) $(OBJECTS)

$(TARGET): 
$(TARGET): $(OBJECTS)
	$(CCACHE) $(CC) $(OBJECTS) $(LDFLAGS) $(LDLIBS) -o $@

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


run:
	./$(TARGET) --no-timer "$(TEST)"

debug: default
	kdbg ./$(TARGET) -a '--rerun-failed --no-timer "$(TEST)"'

valgrind:default
	valgrind $(VALGRIND_FLAGS) ./$(TARGET) --no-timer "$(TEST)"

callgrind: default
	valgrind $(CALLGRIND_FLAGS) ./$(TARGET) --no-timer "$(TEST)"
	kcachegrind ./$(CALLGRIND_OUTFILE)

coverage-gcc: O = 0
coverage-gcc: CC = gcc
coverage-gcc: CFLAGS += -fprofile-arcs -ftest-coverage
coverage-gcc: LDFLAGS += -fprofile-arcs -ftest-coverage
coverage-gcc: default

coverage: O = 0
coverage: CC = clang
coverage: CFLAGS += -fprofile-instr-generate -fcoverage-mapping
coverage: LDFLAGS += -fprofile-instr-generate -fcoverage-mapping
coverage: default

sanitize:CC = clang
sanitize:COLOR_OPT = ""
sanitize:LDFLAGS += -fsanitize=address
sanitize:LIBS += -lubsan
sanitize:CFLAGS += $(SANITIZE_FLAGS)
sanitize:default
