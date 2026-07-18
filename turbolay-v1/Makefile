TYPST ?= typst
ENTRY := main.typ
OUTPUT := main.pdf
TYPST_ROOT := --root ..
MODE ?= dark
TYPST_INPUTS := --input mode=$(MODE)

.PHONY: build watch clean dark light sepia

build:
	$(TYPST) compile $(TYPST_ROOT) $(TYPST_INPUTS) $(ENTRY) $(OUTPUT)

watch:
	$(TYPST) watch $(TYPST_ROOT) $(TYPST_INPUTS) $(ENTRY) $(OUTPUT)

dark:
	$(MAKE) MODE=dark build

light:
	$(MAKE) MODE=light build

sepia:
	$(MAKE) MODE=sepia build

clean:
	rm -f $(OUTPUT)
