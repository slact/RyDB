NAME	:= rydb
SNAME	:= lib$(NAME).a
DNAME	:= lib$(NAME).so
BINARY	:= rydb_test

LIBS	:=
LDFLAGS	:= -rdynamic
CC = clang
O=0
CCACHE = ccache
COLOR = always
COLOR_OPT = -fdiagnostics-color=${COLOR}
CFLAGS = -ggdb -O${O} -Wall -Wextra $(COLOR_OPT) -Wpointer-sign -Wpointer-arith -Wshadow  -Wnested-externs -Wsign-compare -Wpedantic -fPIC
VALGRIND_FLAGS = --tool=memcheck --track-origins=yes --read-var-info=yes --leak-check=full --show-leak-kinds=all --leak-check-heuristics=all --keep-stacktraces=alloc-and-free
SANITIZE_FLAGS = -fsanitize-address-use-after-scope -fno-omit-frame-pointer -fsanitize=address -fsanitize=undefined -fsanitize=shift -fsanitize=integer-divide-by-zero -fsanitize=unreachable -fsanitize=vla-bound -fsanitize=null -fsanitize=return -fsanitize=bounds -fsanitize=alignment -fsanitize=object-size -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fsanitize=nonnull-attribute -fsanitize=returns-nonnull-attribute -fsanitize=enum -fstack-protector
CALLGRIND_FLAGS = --tool=callgrind --collect-jumps=yes  --collect-systime=yes --branch-sim=yes --cache-sim=yes --simulate-hwpref=yes --simulate-wb=yes --callgrind-out-file=callgrind-rydb-%p.out
ANALYZE_FLAGS = -maxloop 10 -enable-checker alpha.clone -enable-checker alpha.core -enable-checker alpha.deadcode -enable-checker alpha.security -enable-checker alpha.unix -enable-checker nullability
CLANG_TIDY_CHECKS = bugprone-*, readability-*, performance-*, google-*, cert-*, -cert-err34-c, -google-readability-todo, -clang-diagnostic-unused-function
TEST_DIR = ./tests

.PHONY: default all clean force valgrind callgrind tidy sanitize

default: all
all: $(SNAME) $(DNAME) $(BINARY)

OBJECTS := $(patsubst %.c, %.o, $(filter-out rydb_test.c, $(wildcard *.c)))
HEADERS := $(wildcard *.h)
ENTRY_OBJECTS:= rydb_test.o

$(OBJECTS): .compiler_flags

%.o: %.c $(HEADERS)
	$(CCACHE) $(CC) $(CFLAGS) -c $< -o $@

.PRECIOUS: $(BINARY) $(OBJECTS) $(SNAME) $(DNAME)

rydb_test: $(OBJECTS) $(ENTRY_OBJECTS)
	$(CCACHE) $(CC) $(OBJECTS) $(ENTRY_OBJECTS) $(LIBS) $(LDFLAGS) -o $@

$(SNAME): $(OBJECTS)
	$(AR) $(ARFLAGS) $@ $^

$(DNAME): LDFLAGS += -shared
$(DNAME): $(OBJECTS)
	$(CCACHE) $(CC) $(OBJECTS) $(LIBS) $(LDFLAGS) -o $@

clean:
	-rm -f *.o
	-rm -f *.gcda
	-rm -f *.gcno
	-rm -f tests/*.gcda
	-rm -f tests/*.gcno
	-rm -f $(BINARY)
	-rm -f $(SNAME)
	-rm -f $(DNAME)
	-rm -f tests/*.o
	-rm -f tests/test
	-rm -f coverage.*
	-rm -Rf tests/test.db.*
	-rm -f *.profraw
	-rm -r tests/*.profraw
	-rm -Rf coverage-report

.compiler_flags: force
	@echo '$(CC) $(CFLAGS)' | cmp -s - $@ || echo '$(CC) $(CFLAGS)' > $@

gcc5:	CC = gcc-5
gcc5:	CFLAGS += -fvar-tracking-assignments
gcc5:	default

gcc:	CC = gcc
gcc:	CFLAGS += -fvar-tracking-assignments -Wimplicit-fallthrough
gcc:	default

clang:	CC = clang
clang:	default

analyze:clean
	scan-build $(ANALYZE_FLAGS) --view -stats $(MAKE) O=$(O) CC=clang CFLAGS="$(CFLAGS)" CCACHE=""

sanitize:CC = clang
sanitize:COLOR_OPT = ""
sanitize:LDFLAGS = -fsanitize=address
sanitize:LIBS += -lubsan
sanitize:CFLAGS += $(SANITIZE_FLAGS)
sanitize:default

tidy:   clean default
	clang-tidy -checks="$(CLANG_TIDY_CHECKS)" *.c

lib:	$(DNAME)

test:   CFLAGS += -DRYDB_DEBUG
test:	$(DNAME)
	$(MAKE) -C $(TEST_DIR)
	$(MAKE) -C $(TEST_DIR) run

coverage-gcc-create: O = 0
coverage-gcc-create: CC = gcc
coverage-gcc-create: CFLAGS += -fprofile-arcs -ftest-coverage
coverage-gcc-create: LDFLAGS += -fprofile-arcs -ftest-coverage
coverage-gcc-create: $(DNAME)
coverage-gcc-create:
	$(MAKE) -C $(TEST_DIR) coverage-gcc
	$(MAKE) -C $(TEST_DIR) run

coverage-gcc: coverage-gcc-create
	gcovr --html-details -o coverage.html
	xdg-open ./coverage.html

coverage-gcc-gcov: coverage-gcc-create
	gcov  $(filter-out rydb_test.c, $(wildcard *.c)) $(HEADERS)

coverage: O = 0
coverage: CC = clang
coverage: CFLAGS += -fprofile-instr-generate -fcoverage-mapping
coverage: LDFLAGS += -fprofile-instr-generate -fcoverage-mapping
coverage: $(DNAME)
coverage: 
	$(MAKE) -C $(TEST_DIR) coverage
	LLVM_PROFILE_FILE=".profraw" $(MAKE) -C $(TEST_DIR) run
	llvm-profdata merge -sparse $(TEST_DIR)/.profraw -o .profdata
	llvm-cov show -format="html" -output-dir="coverage-report" -instr-profile=".profdata" "librydb.so" -object "tests/test"
	xdg-open ./coverage-report/index.html

debug:	$(DNAME)
	$(MAKE) -C $(TEST_DIR) debug
valgrind:	$(DNAME)
	$(MAKE) -C $(TEST_DIR) valgrind
sanitize:	sanitize
	$(MAKE) -C $(TEST_DIR) sanitize
	$(MAKE) -C $(TEST_DIR) run
callgrind: $(DNAME)
	$(MAKE) -C $(TEST_DIR) callgrind
