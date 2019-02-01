TARGET = rydb_test
LIBS = 
LDEXTRAFLAGS=-rdynamic
CC = clang
O=0
CFLAGS =-ggdb -O${O} -Wall -Wextra -Wpointer-sign -Wpointer-arith -Wshadow  -Wnested-externs -Wsign-compare
VALGRIND_FLAGS = --tool=memcheck --track-origins=yes --read-var-info=yes --leak-check=full --show-leak-kinds=all --leak-check-heuristics=all --keep-stacktraces=alloc-and-free
SANITIZE_FLAGS = -fsanitize=undefined -fsanitize=shift -fsanitize=integer-divide-by-zero -fsanitize=unreachable -fsanitize=vla-bound -fsanitize=null -fsanitize=return -fsanitize=bounds -fsanitize=alignment -fsanitize=object-size -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fsanitize=nonnull-attribute -fsanitize=returns-nonnull-attribute -fsanitize=enum
CALLGRIND_FLAGS = --tool=callgrind --collect-jumps=yes  --collect-systime=yes --branch-sim=yes --cache-sim=yes --simulate-hwpref=yes --simulate-wb=yes --callgrind-out-file=callgrind-rydb-%p.out
ANALYZE_FLAGS = -maxloop 10 -enable-checker alpha.clone -enable-checker alpha.core -enable-checker alpha.deadcode -enable-checker alpha.security -enable-checker alpha.unix

.PHONY: default all clean force test

default: $(TARGET)
all: default

OBJECTS = $(patsubst %.c, %.o, $(wildcard *.c))
HEADERS = $(wildcard *.h)

$(OBJECTS): .compiler_flags

%.o: %.c $(HEADERS)
	$(CC) $(CFLAGS) -c $< -o $@

.PRECIOUS: $(TARGET) $(OBJECTS)

$(TARGET): $(OBJECTS)
	$(CC) $(OBJECTS) -Wall $(LIBS) $(LDEXTRAFLAGS) -o $@

clean:
	-rm -f *.o
	-rm -f $(TARGET)

.compiler_flags: force
	@echo '$(CC) $(CFLAGS)' | cmp -s - $@ || echo '$(CC) $(CFLAGS)' > $@

gcc5:
	$(MAKE) CC=gcc-5 CFLAGS="$(CFLAGS) -fvar-tracking-assignments" O=$(O)
gcc:
	$(MAKE) CC=gcc CFLAGS="$(CFLAGS) -fvar-tracking-assignments" O=$(O)
clang:
	$(MAKE) CC=clang O=$(O)
analyze: clean
	scan-build $(ANALYZE_FLAGS) --view -stats $(MAKE) O=$(O)
sanitize:
	$(MAKE) CC=clang CFLAGS="$(CFLAGS) $(SANITIZE_FLAGS)" LIBS="$(LIBS) -lubsan" O=$(O)
test: default
	./$(TARGET)
debug: default
	sudo kdbg ./$(TARGET)
valgrind: gcc5
	valgrind $(VALGRIND_FLAGS)  ./$(TARGET)
callgrind: default
	valgrind $(CALLGRIND_FLAGS)  ./$(TARGET)
