TARGET = rydb_test
LIBS = 
LDEXTRAFLAGS=-rdynamic
CC = clang
O=0
CFLAGS =-ggdb -O${O} -Wall -Wextra -Wpointer-sign -Wpointer-arith -Wshadow  -Wnested-externs -Wsign-compare
VALGRIND_FLAGS = --tool=memcheck --track-origins=yes --read-var-info=yes --leak-check=full --show-leak-kinds=all --leak-check-heuristics=all --keep-stacktraces=alloc-and-free
SANITIZE_FLAGS = -fsanitize=undefined -fsanitize=shift -fsanitize=integer-divide-by-zero -fsanitize=unreachable -fsanitize=vla-bound -fsanitize=null -fsanitize=return -fsanitize=bounds -fsanitize=alignment -fsanitize=object-size -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fsanitize=nonnull-attribute -fsanitize=returns-nonnull-attribute -fsanitize=enum
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
	$(MAKE) CC=gcc-5 CFLAGS="$(CFLAGS) -fvar-tracking-assignments"
gcc:
	$(MAKE) CC=gcc CFLAGS="$(CFLAGS) -fvar-tracking-assignments"
clang:
	$(MAKE) CC=clang
analyze:
	scan-build --view -stats $(MAKE)
sanitize:
	@$(MAKE) CC=clang CFLAGS="$(CFLAGS) $(SANITIZE_FLAGS)" LIBS="$(LIBS) -lubsan"
test: default
	./$(TARGET)
debug: default
	sudo kdbg ./$(TARGET)
valgrind: default
	valgrind $(VALGRIND_FLAGS)  ./$(TARGET)
