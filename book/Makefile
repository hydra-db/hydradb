# HydraDB Design Docs — Typst book.
#
#   make index   regenerate chapters/_index.typ from the chapters/ folder
#   make build   regenerate the index, then compile main.typ -> main.pdf
#   make watch    live-rebuild while editing
#   make dark / make sepia   themed PDFs
#
# `index` is what keeps the book auto-updated: it runs scripts/gen-index.sh,
# which scans chapters/ and rewrites the #part/#include list. `build` always
# runs `index` first so a newly added chapter file is never missed.

TYPST ?= typst
MAIN  := main.typ

.PHONY: index build watch dark sepia overview clean

index:
	./scripts/gen-index.sh

build: index
	$(TYPST) compile $(MAIN) main.pdf

# Standalone one-document executive overview (does not include chapters/).
overview:
	$(TYPST) compile overview.typ overview.pdf

watch: index
	$(TYPST) watch $(MAIN) main.pdf

dark: index
	$(TYPST) compile --input mode=dark $(MAIN) main-dark.pdf

sepia: index
	$(TYPST) compile --input mode=sepia $(MAIN) main-sepia.pdf

clean:
	rm -f main.pdf main-dark.pdf main-sepia.pdf overview.pdf
