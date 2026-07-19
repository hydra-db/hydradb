TYPST ?= typst
MODE ?= dark
TYPST_INPUTS := --input mode=$(MODE)

CONCEPTUAL_ENTRY := conceptual.typ
CONCEPTUAL_OUTPUT := conceptual.pdf

INSIDE_ENTRY := inside.typ
INSIDE_OUTPUT := inside.pdf

.PHONY: index build conceptual inside watch-conceptual watch-inside dark light sepia clean

# Regenerate chapters/_index.typ for the conceptual guide (sweeps intro-*/detail-*).
index:
	bash scripts/gen-index.sh

# Build both new books.
build: conceptual inside

conceptual: index
	$(TYPST) compile $(TYPST_INPUTS) $(CONCEPTUAL_ENTRY) $(CONCEPTUAL_OUTPUT)

inside:
	$(TYPST) compile $(TYPST_INPUTS) $(INSIDE_ENTRY) $(INSIDE_OUTPUT)

watch-conceptual: index
	$(TYPST) watch $(TYPST_INPUTS) $(CONCEPTUAL_ENTRY) $(CONCEPTUAL_OUTPUT)

watch-inside:
	$(TYPST) watch $(TYPST_INPUTS) $(INSIDE_ENTRY) $(INSIDE_OUTPUT)

dark:
	$(MAKE) MODE=dark build

light:
	$(MAKE) MODE=light build

sepia:
	$(MAKE) MODE=sepia build

clean:
	rm -f $(CONCEPTUAL_OUTPUT) $(INSIDE_OUTPUT)
