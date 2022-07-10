.phony: all docs check clean

OUTDIR = ./build

RUSTC = rustc -O -L $(OUTDIR) --out-dir $(OUTDIR)

all: docs $(OUTDIR)
	$(RUSTC) src/papi/lib.rs

check: all
	$(RUSTC) --test src/papi/test.rs
	$(OUTDIR)/test

docs:
	rustdoc src/papi/lib.rs

$(OUTDIR):
	mkdir -p $(OUTDIR)
