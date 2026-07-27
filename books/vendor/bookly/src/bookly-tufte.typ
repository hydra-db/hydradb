#import "@preview/marginalia:0.3.1" as marginalia: note, notefigure, wideblock
#import "bookly-defaults.typ": *
#import "bookly-helper.typ": *

#let tufte-content(body) = block(width: 5cm, body)
#let margin-factor = 1.4

#let _note-base = note.with(
  counter: states.sidenotecounter,
  numbering: (..i) => super(numbering("1", ..i)),
  keep-order: true
)

// Sidenotes live in a narrow margin column, where the default hyphenation
// produces ugly mid-word breaks (e.g. "cont-rast"). Disable hyphenation for
// note bodies so whole words wrap instead. Applies to every #note[...] call.
#let note(..args) = {
  let pos = args.pos()
  _note-base(..pos.slice(0, -1), ..args.named(), text(hyphenate: false, pos.last()))
}

#let notefigure = notefigure.with(keep-order: true)

#let notecite(key, supplement: none, dy: 0em, alignment: "baseline") = context {
  let elems = query(bibliography)
  if elems.len() > 0 {
    cite(key, supplement: supplement)
    note(
      counter: none,
      dy: dy,
      alignment: alignment,
      keep-order: true,
      cite(key, form: "full", style: "resources/short_ref.csl"))
  } else {
    panic("No bibliography found. Please add a bibliography to use notecite.")
  }
}
