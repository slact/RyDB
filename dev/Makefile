NAME	:= RyDB
SNAME	:= lib$(NAME).a
DNAME	:= lib$(NAME).so

LIBS	:=
LDFLAGS	:= -rdynamic
CC = clang
O=0
CCACHE = ccache
COLOR = always
COLOR_OPT = -fdiagnostics-color=${COLOR}
RYDB_DEBUG = 1
CFLAGS = -ggdb -I $(SRC_DIR) -O${O} -Wall -Wextra $(COLOR_OPT) -Wpointer-sign -Wpointer-arith -Wshadow  -Wnested-externs -Wsign-compare -Wpedantic -fPIC
VALGRIND_FLAGS = --tool=memcheck --track-origins=yes --read-var-info=yes --leak-check=full --show-leak-kinds=all --leak-check-heuristics=all --keep-stacktraces=alloc-and-free
SANITIZE_ADDRESS_USE = -fsanitize-address-use-after-scope
SANITIZE_FLAGS = $(SANITIZE_ADDRESS_USE) -fno-omit-frame-pointer -fsanitize=address -fsanitize=undefined -fsanitize=shift -fsanitize=integer-divide-by-zero -fsanitize=unreachable -fsanitize=vla-bound -fsanitize=null -fsanitize=return -fsanitize=bounds -fsanitize=alignment -fsanitize=object-size -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fsanitize=nonnull-attribute -fsanitize=returns-nonnull-attribute -fsanitize=enum -fstack-protector
CALLGRIND_FLAGS = --tool=callgrind --collect-jumps=yes  --collect-systime=yes --branch-sim=yes --cache-sim=yes --simulate-hwpref=yes --simulate-wb=yes --callgrind-out-file=callgrind-rydb-%p.out
ANALYZE_FLAGS = -maxloop 100 -enable-checker alpha.clone -enable-checker alpha.core -enable-checker alpha.deadcode -enable-checker alpha.security -enable-checker alpha.unix -enable-checker nullability
CLANG_TIDY_CHECKS = bugprone-*, readability-*, performance-*, google-*, cert-*, -cert-err34-c, -google-readability-todo, -clang-diagnostic-unused-function
TEST_DIR = ../tests
SRC_DIR = ../src
.PHONY: default all clean force valgrind callgrind tidy sanitize cmake

CMakeCache = ../CMakeCache.txt

default: all
all: $(SNAME) $(DNAME) $(BINARY)


OBJECTS := $(patsubst $(SRC_DIR)/%.c, $(SRC_DIR)/%.o, $(filter-out rydb_test.c, $(wildcard $(SRC_DIR)/*.c)))
HEADERS := $(wildcard $(SRC_DIR)/*.h)
# 
$(OBJECTS): .compiler_flags

%.o: %.c $(HEADERS)
	$(CCACHE) $(CC) $(CFLAGS) -c $< -o $@

.PRECIOUS: $(BINARY) $(OBJECTS) $(SNAME) $(DNAME)

$(SNAME): $(CMakeCache) $(OBJECTS)
	$(AR) $(ARFLAGS) ../$(SNAME) $^

$(DNAME): LDFLAGS += -shared
$(DNAME): $(CMakeCache) $(OBJECTS)
	$(CCACHE) $(CC) $(OBJECTS) $(LIBS) $(LDFLAGS) -o ../$(DNAME)

clean:
	-rm -Rf ../CMakeFiles
	-rm -f ../CMakeCache.txt
	-rm -f $(SRC_DIR)/configure.h
	-rm -f $(SRC_DIR)/*.o
	-rm -f $(SRC_DIR)/*.gcda
	-rm -f $(SRC_DIR)/*.gcno
	-rm -f $(TEST_DIR)/*.gcda
	-rm -f $(TEST_DIR)/*.gcno
	-rm -f $(SRC_DIR)/*.so
	-rm -f $(SRC_DIR)/*.a
	-rm -f $(TEST_DIR)/*.o
	-rm -f $(TEST_DIR)/test
	-rm -f coverage.*
	-rm -Rf $(TEST_DIR)/test.db.*
	-rm -f $(SRC_DIR)/*.profraw
	-rm -f $(TEST_DIR)/*.profraw
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

tidy:   clean default
	clang-tidy -checks="$(CLANG_TIDY_CHECKS)" *.c

lib:	$(DNAME)

test:	$(DNAME)
	$(MAKE) -C $(TEST_DIR)
	$(MAKE) -C $(TEST_DIR) run

$(CMakeCache):
	if test -z "$(RYDB_DEBUG)"; then \
		cd ../ && ./configure --disable-rydb-debug; \
	else \
		cd ../ && ./configure --enable-rydb-debug; \
	fi

cmake:
	cd ../ && cmake ./ && $(MAKE)

cmake-test: cmake
	$(MAKE) -C $(TEST_DIR)
	$(MAKE) -C $(TEST_DIR) run

coverage-gcc-create: O = 0
coverage-gcc-create: CC = gcc
coverage-gcc-create: CFLAGS += -fprofile-arcs -ftest-coverage
coverage-gcc-create: LDFLAGS += -fprofile-arcs -ftest-coverage
coverage-gcc-create: $(DNAME)
coverage-gcc-create:
	$(MAKE) -C $(TEST_DIR) coverage-gcc
	-$(MAKE) -C $(TEST_DIR) run

coverage-gcc: coverage-gcc-create
	gcovr --html-details --exclude tests/snow.h --exclude tests/ --exclude rbtree.c --exclude rbtree.h -o coverage.html
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
	-LLVM_PROFILE_FILE=".profraw" $(MAKE) -C $(TEST_DIR) run
	llvm-profdata merge -sparse $(TEST_DIR)/.profraw -o .profdata
	llvm-cov show -format="html" -output-dir="coverage-report" -instr-profile=".profdata" "librydb.so" -object "tests/test" -ignore-filename-regex="tests/|rbtree"
	xdg-open ./coverage-report/index.html

debug:	$(DNAME)
	$(MAKE) -C $(TEST_DIR) debug
valgrind:	$(DNAME)
	$(MAKE) -C $(TEST_DIR) MULTIPLIER=0.1 valgrind

sanitize:CC = clang
sanitize:COLOR_OPT = ""
sanitize:LDFLAGS = -fsanitize=address
sanitize:LIBS += -lubsan
sanitize:CFLAGS += $(SANITIZE_FLAGS) $(SANITIZE_EXTRA_FLAGS)
sanitize:default
sanitize:
	$(MAKE) -C $(TEST_DIR) sanitize
	$(MAKE) -C $(TEST_DIR) MULTIPLIER=0.4 run

callgrind: $(DNAME)
	$(MAKE) -C $(TEST_DIR) callgrind
