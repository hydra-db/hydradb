TYPST ?= typst
ENTRY := main.typ
OUTPUT := main.pdf
MODE ?= dark
TYPST_INPUTS := --input mode=$(MODE)

QUINT_ENTRY := quint.typ
QUINT_OUTPUT := quint.pdf

.PHONY: index build watch clean dark light sepia quint quint-watch

index:
	bash scripts/gen-index.sh

build: index
	$(TYPST) compile $(TYPST_INPUTS) $(ENTRY) $(OUTPUT)

watch: index
	$(TYPST) watch $(TYPST_INPUTS) $(ENTRY) $(OUTPUT)

# Standalone "Verifying turbolay" (Quint) book. Its chapters use the quint-*
# prefix so gen-index.sh does NOT sweep them into main.typ — hence no index dep.
# Defaults to dark (MODE ?= dark); override with `make MODE=light quint`.
quint:
	$(TYPST) compile $(TYPST_INPUTS) $(QUINT_ENTRY) $(QUINT_OUTPUT)

quint-watch:
	$(TYPST) watch $(TYPST_INPUTS) $(QUINT_ENTRY) $(QUINT_OUTPUT)

dark:
	$(MAKE) MODE=dark build

light:
	$(MAKE) MODE=light build

sepia:
	$(MAKE) MODE=sepia build

clean:
	rm -f $(OUTPUT) $(QUINT_OUTPUT)
