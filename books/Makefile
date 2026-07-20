TYPST ?= typst
MODE ?= dark
TYPST_INPUTS := --input mode=$(MODE)

# The three TurboLay books, all authored the same way (see AUTHORING-NOTES.md).
CONCEPTUAL_ENTRY := conceptual.typ
CONCEPTUAL_OUTPUT := conceptual.pdf

INSIDE_ENTRY := inside.typ
INSIDE_OUTPUT := inside.pdf

QUINT_ENTRY := quint.typ
QUINT_OUTPUT := quint.pdf

.PHONY: index build conceptual inside quint \
	watch-conceptual watch-inside watch-quint dark light sepia clean

# Regenerate chapters/_index.typ for the conceptual guide (sweeps intro-*/detail-*).
# quint-* chapters are included explicitly by quint.typ, so the sweep ignores them.
index:
	bash scripts/gen-index.sh

# Build all three books.
build: conceptual inside quint

conceptual: index
	$(TYPST) compile $(TYPST_INPUTS) $(CONCEPTUAL_ENTRY) $(CONCEPTUAL_OUTPUT)

inside:
	$(TYPST) compile $(TYPST_INPUTS) $(INSIDE_ENTRY) $(INSIDE_OUTPUT)

quint:
	$(TYPST) compile $(TYPST_INPUTS) $(QUINT_ENTRY) $(QUINT_OUTPUT)

watch-conceptual: index
	$(TYPST) watch $(TYPST_INPUTS) $(CONCEPTUAL_ENTRY) $(CONCEPTUAL_OUTPUT)

watch-inside:
	$(TYPST) watch $(TYPST_INPUTS) $(INSIDE_ENTRY) $(INSIDE_OUTPUT)

watch-quint:
	$(TYPST) watch $(TYPST_INPUTS) $(QUINT_ENTRY) $(QUINT_OUTPUT)

dark:
	$(MAKE) MODE=dark build

light:
	$(MAKE) MODE=light build

sepia:
	$(MAKE) MODE=sepia build

clean:
	rm -f $(CONCEPTUAL_OUTPUT) $(INSIDE_OUTPUT) $(QUINT_OUTPUT)
