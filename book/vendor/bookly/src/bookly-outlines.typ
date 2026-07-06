#import "@preview/marginalia:0.3.1" as marginalia: wideblock
#import "bookly-helper.typ": *

// Table of contents
#let tableofcontents = {
  show outline.entry: it => context {
    if states.tufte.get() {
      wideblock(side: "both")[#it]
    } else {
      it
    }
  }
  outline(title: context states.localization.get().toc, indent: 1em)
}

// List of figures
#let listoffigures = context {
  show outline.entry: it => context {
    let entry = context {
      let prev-outline-state = states.in-outline.get()
      states.in-outline.update(true)
      it
      states.in-outline.update(prev-outline-state)
    }

    if states.tufte.get() {
      wideblock(side: "both")[#entry]
    } else {
      entry
    }
  }
  outline(title: context states.localization.get().lof, target: figure.where(kind: image))
}

// List of tables
#let listoftables = context {
  show outline.entry: it => context {
    let entry = context {
      let prev-outline-state = states.in-outline.get()
      states.in-outline.update(true)
      it
      states.in-outline.update(prev-outline-state)
    }

    if states.tufte.get() {
      wideblock(side: "both")[#entry]
    } else {
      entry
    }
  }
  outline(title: context states.localization.get().lot, target: figure.where(kind: table))
}
