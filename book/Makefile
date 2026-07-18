TYPST ?= typst
ENTRY := main.typ
OUTPUT := main.pdf
MODE ?= dark
TYPST_INPUTS := --input mode=$(MODE)

.PHONY: index build watch clean dark light sepia

index:
	bash scripts/gen-index.sh

build: index
	$(TYPST) compile $(TYPST_INPUTS) $(ENTRY) $(OUTPUT)

watch: index
	$(TYPST) watch $(TYPST_INPUTS) $(ENTRY) $(OUTPUT)

dark:
	$(MAKE) MODE=dark build

light:
	$(MAKE) MODE=light build

sepia:
	$(MAKE) MODE=sepia build

clean:
	rm -f $(OUTPUT)
