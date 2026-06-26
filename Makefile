EXTENSION    = $(shell grep -m 1 '"name":' META.json | \
               sed -e 's/[[:space:]]*"name":[[:space:]]*"\([^"]*\)",/\1/')
EXTVERSION   = $(shell grep -m 1 'default_version' pg_clickhouse.control | \
               sed -e "s/[[:space:]]*default_version[[:space:]]*=[[:space:]]*'\([^']*\)',\{0,1\}/\1/")
DISTVERSION  = $(shell grep -m 1 '^[[:space:]]\{2\}"version":' META.json | \
               sed -e 's/[[:space:]]*"version":[[:space:]]*"\([^"]*\)",\{0,1\}/\1/')

DATA         = $(sort $(wildcard sql/$(EXTENSION)--*.sql) sql/$(EXTENSION)--$(EXTVERSION).sql)
DOCS         = $(wildcard doc/*.md)
TESTS        ?= $(wildcard test/sql/*.sql)
REGRESS      = --schedule test/schedule
REGRESS_OPTS = --inputdir=test --load-extension=$(EXTENSION)
PG_CONFIG   ?= pg_config
MODULE_big   = $(EXTENSION)
CURL_CONFIG ?= curl-config
OS 	        ?= $(shell uname -s | tr A-Z a-z)
ARCH         = $(shell uname -m)

# Collect all the C files to compile into MODULE_big. src/jit/ is the OPTIONAL
# LLVM-JIT module (pg_clickhouse_jit.so), built separately with LLVM flags and
# linked on its own -- it must NOT be swept into the LLVM-free base extension.
OBJS = $(subst .c,.o, $(filter-out src/jit/%, $(wildcard src/*.c src/*/*.c)))
# C++ translation units (templated column-major deform kernels + extern "C" driver).
OBJS += $(patsubst %.cpp,%.o, $(filter-out src/jit/%, $(wildcard src/*.cpp src/*/*.cpp)))

# clickhouse-c is a header-only single-header library. Override
# CH_C_DIR to point elsewhere when developing against a local checkout.
CH_C_DIR ?= vendor/clickhouse-c

# Add include directories.
PG_CPPFLAGS = -I./src/include -I$(CH_C_DIR)

# Link OpenSSL (for TLS in the binary driver), curl (for the HTTP driver),
# libuuid (for http_streaming.c's query-id generator), and lz4 / zstd
# (for the binary driver's compressed-frame codecs).
PG_LDFLAGS = -lssl -lcrypto -llz4 -lzstd $(shell $(CURL_CONFIG) --libs)

# libuuid is provided by the OS on darwin; explicit link elsewhere. librt
# provides shm_open/shm_unlink for the SHM producer (shm_producer.c) on Linux.
ifneq ($(OS),darwin)
	PG_LDFLAGS += -luuid -lrt
endif

# Hot-Cold Phase 3 Branch P1 (D-HC-0302): the TCP-transport producer's socket send is a plain
# non-blocking send() with epoll(EPOLLOUT) readiness backpressure (no io_uring / no liburing
# dependency). io_uring was used synchronously (submit-then-wait, one send in flight), so epoll is a
# loopback wash; the switch buys simplicity (no ring lifetime), robustness (epoll has no seccomp-disabled
# fallback degradation), and portability. msg_zerocopy (send(MSG_ZEROCOPY)+errqueue) survives unchanged.

# nanoarrow (Hot-Cold Phase 2, Branch A): the TCP-transport producer serialises
# each block as an Apache Arrow IPC encapsulated message (Schema + RecordBatch)
# via the vendored nanoarrow + nanoarrow_ipc amalgamation (src/nanoarrow/, v0.8.0)
# so the wire speaks a standard columnar format the stock ClickHouse Arrow reader
# can decode (decision D-HC-0202). The three vendored TUs live one level deeper
# than the src/*/*.c auto-glob, so they are added to OBJS explicitly and compiled
# with warnings relaxed (third-party generated code) -- the base extension stays
# -Wall -Werror. Guarded by the vendored header (no hard dependency); the Arrow
# producer path is #ifdef PGCH_USE_NANOARROW.
NANOARROW_DIR = src/nanoarrow
ifneq ($(wildcard $(NANOARROW_DIR)/include/nanoarrow/nanoarrow_ipc.h),)
	PG_CPPFLAGS += -DPGCH_USE_NANOARROW -I./$(NANOARROW_DIR)/include
	NANOARROW_OBJS = $(NANOARROW_DIR)/src/nanoarrow.o \
	                 $(NANOARROW_DIR)/src/nanoarrow_ipc.o \
	                 $(NANOARROW_DIR)/src/flatcc.o
	OBJS += $(NANOARROW_OBJS)
endif

# Suppress annoying pre-c99 warning, error on other warnings, include curl.
PG_CFLAGS = -Wno-declaration-after-statement -Wall -Werror $(shell $(CURL_CONFIG) --cflags)

# C++ TU (src/shm_deform.cpp): C++20, no exceptions/RTTI (allocation-free,
# trivial-destructor templated kernels), same -Wall -Werror discipline.
PG_CXXFLAGS = -std=c++20 -fno-exceptions -fno-rtti -Wall -Werror

# Clean up generated files.
EXTRA_CLEAN = sql/$(EXTENSION)--$(EXTVERSION).sql src/include/version.h compile_commands.json test/schedule $(EXTENSION)-$(DISTVERSION).zip

# Import PGXS.
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Compile the vendored nanoarrow/flatcc amalgamation without -Wall -Werror (it is
# third-party generated code); keeps optimisation/PIC from the PGXS CFLAGS. The
# -w is appended last so it wins over the base extension's -Werror.
ifneq ($(NANOARROW_OBJS),)
$(NANOARROW_OBJS): CFLAGS += -w
EXTRA_CLEAN += $(NANOARROW_OBJS)
endif

# ---- Optional LLVM-JIT deform module (pg_clickhouse_jit) ----------------
# Built ONLY when an llvm-config is found. The base extension never links LLVM;
# it loads this module lazily (load_external_function) and falls back to the AOT
# step plan when it is absent. Self-contained: links libLLVM + libstdc++ only
# (reuses the system libLLVM that postgresql-NN-jit already ships). Kept out of
# MODULE_big's OBJS so the main .so stays LLVM-free.
LLVM_CONFIG ?= $(shell command -v llvm-config-21 llvm-config 2>/dev/null | head -n1)
ifneq ($(LLVM_CONFIG),)
JIT_MODULE   := pg_clickhouse_jit$(DLSUFFIX)
JIT_CXXFLAGS := -O2 -fno-exceptions -fno-rtti -fPIC -fvisibility=hidden \
  $(shell $(LLVM_CONFIG) --cxxflags | sed 's/-std=c++[0-9]*//') -std=c++17 \
  -I./src/include -I$(shell $(PG_CONFIG) --includedir-server) -I$(shell $(PG_CONFIG) --includedir)
JIT_LDFLAGS  := $(shell $(LLVM_CONFIG) --ldflags) -Wl,-rpath,$(shell $(LLVM_CONFIG) --libdir) \
  $(shell $(LLVM_CONFIG) --libs orcjit native) $(shell $(LLVM_CONFIG) --system-libs)

all: $(JIT_MODULE)
$(JIT_MODULE): src/jit/pgch_jit.cpp src/include/pgch_jit.h src/include/shm_deform.h
	clang++ $(JIT_CXXFLAGS) -shared -o $@ $< $(JIT_LDFLAGS)

install: install-pgch-jit
.PHONY: install-pgch-jit
install-pgch-jit: $(JIT_MODULE)
	$(INSTALL_SHLIB) $(JIT_MODULE) '$(DESTDIR)$(pkglibdir)/$(JIT_MODULE)'

EXTRA_CLEAN += $(JIT_MODULE)
endif

# Clone clickhouse-c submodule.
$(CH_C_DIR)/clickhouse.h: .gitmodules
	git submodule update --init

# Require clickhouse-c and the version header.
$(OBJS): $(CH_C_DIR)/clickhouse.h src/include/version.h

# Require the versioned C source and SQL script.
all: sql/$(EXTENSION)--$(EXTVERSION).sql

# Versioned SQL script.
sql/$(EXTENSION)--$(EXTVERSION).sql: sql/$(EXTENSION).sql
	cp $< $@

# Versioned source file.
src/include/version.h: src/include/version.h.in
	sed -e 's,__VERSION__,$(DISTVERSION),g' $< > $@

# Build a PGXN distribution bundle.
dist: $(EXTENSION)-$(DISTVERSION).zip

$(EXTENSION)-$(DISTVERSION).zip:
	git archive-all -v --prefix "$(EXTENSION)-$(DISTVERSION)/" --force-submodules $(EXTENSION)-$(DISTVERSION).zip

.PHONY: test/schedule # Depends on $(TESTS), so always rebuild.
test/schedule:
	@echo "test: $(patsubst test/sql/%.sql,%,$(TESTS))" > $@

installcheck: test/schedule

# Test the PGXN distribution.
dist-test: $(EXTENSION)-$(DISTVERSION).zip
	unzip $(EXTENSION)-$(DISTVERSION).zip
	cd $(EXTENSION)-$(DISTVERSION)
	make && make install && make installcheck

.PHONY: release-notes # Show release notes for current version (must have `mknotes` in PATH).
release-notes: CHANGELOG.md
	mknotes -v v$(DISTVERSION) -f $< -r https://github.com/$(or $(GITHUB_REPOSITORY),ClickHouse/pg_clickhouse)

.PHONY: tempcheck # Run tests with a temporary PostgreSQL instance
tempcheck: install
	$(pg_regress_installcheck) --temp-instance=/tmp/pg_clickhouse_test $(REGRESS_OPTS) $(REGRESS)

# Run `make installcheck` and copy all result files to test/expected/. Use for
# basic test changes with the latest version of Postgres, but be aware that
# alternate `_n.out` files will not be updated.
#
# DO NOT RUN UNLESS YOU'RE CERTAIN ALL YOUR TESTS ARE PASSING!
.PHONY: results
results:
	$(MAKE) installcheck || true
	rsync -rlpgovP results/ test/expected

# Run make print-VARIABLE_NAME to print VARIABLE_NAME's flavor and value.
print-%	: ; $(info $* is $(flavor $*) variable set to "$($*)") @true

# OCI images.
REGISTRY ?= localhost:5001
REVISION := $(shell git rev-parse --short HEAD)
PLATFORMS ?= linux/amd64,linux/arm64
PG_VERSIONS ?= 18,17,16,15,14,13
.PHONY: image # Build the linux/amd64 OCI image.
image:
	registry=$(REGISTRY) version=$(DISTVERSION) revision=$(REVISION) pg_versions=$(PG_VERSIONS) \
	docker buildx bake --set "*.platform=$(PLATFORMS)" \
	$(if $(filter true,$(PUSH)),--push,) \
	$(if $(filter true,$(LOAD)),--load,) \

bake-vars:
	@echo "registry=$(REGISTRY)"
	@echo "version=$(DISTVERSION)"
	@echo "revision=$(REVISION)"
	@echo "pg_versions=$(PG_VERSIONS)"

# Format the .c and .h files according to the PostgreSQL indentation
# standard. Requires `pg_bsd_indent` to be in the path.
indent: dev/indent.sh
	@$<

# Linting.
.PHONY: lint # Lint the project
lint: .pre-commit-config.yaml
	@pre-commit run --show-diff-on-failure --color=always --all-files

.PHONY: clang-tidy # Run clang-tidy static analysis (requires compile_commands.json)
clang-tidy: compile_commands.json
	run-clang-tidy -p . $(wildcard src/*.c src/*/*.c)

## .git/hooks/pre-commit: Install the pre-commit hook
.git/hooks/pre-commit:
	@printf "#!/bin/sh\nmake lint\n" > $@
	@chmod +x $@

debian-install-lint:
	@curl -SsLo /tmp/pre-commit.pyz https://github.com/pre-commit/pre-commit/releases/download/v4.6.0/pre-commit-4.6.0.pyz
	@printf "#!/bin/sh\npython3 /tmp/pre-commit.pyz \"\$$@\"\n" > /usr/local/bin/pre-commit
	@chmod +x /usr/local/bin/pre-commit

.PHONY: lsp # Generate compile_commands.json for IDE/clangd support.
lsp: compile_commands.json

# Requires https://github.com/rizsotto/Bear.
compile_commands.json:
	$(MAKE) clean -j $$(nproc)
	bear --config "dev/bear.$$(if [ "$$(bear --version | awk -F'[^0-9]+' '{ print $$2 }')" -eq 3 ]; then echo 'json'; else echo 'yml'; fi)" -- $(MAKE) all -j $$(nproc)

# ClickHouse Docker Containers
start-containers: dev/Makefile dev/docker-compose.yml
	@$(MAKE) -C dev start

stop-containers: dev/Makefile dev/docker-compose.yml
	@$(MAKE) -C dev stop
