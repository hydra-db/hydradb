#import "bookly-defaults.typ": *
#import "themes/classic.typ": classic
#import "themes/fancy.typ": fancy
#import "themes/modern.typ": modern
#import "themes/obook.typ": obook
#import "themes/orly.typ": orly
#import "themes/pretty.typ": pretty
#import "themes/reader.typ": reader, reader-mode, reader-palettes, reader-colors, reader-title-page, codeblock, ok, bad, warn

// Part
#let part(title) = context (states.theme.get().part)(title)

// Mini table of contents
#let minitoc = context states.theme.get().minitoc

// Boxed equation
#let boxeq(content) = context (states.theme.get().boxeq)(content)

// Custom box
#let custom-box(title: none, icon: "info", color: rgb(29, 144, 208), body) = context (states.theme.get().box)(title: title, icon: icon, color: color, body)

// Information box
#let info-box = custom-box.with(title: context states.localization.get().note)

// Tip box
#let tip-box = custom-box.with(title: context states.localization.get().tip, icon: "tip", color: rgb(0, 166, 81))

// Warning box
#let warning-box = custom-box.with(title: context states.localization.get().warning, icon: "alert", color: orange)

// Important box
#let important-box = custom-box.with(title: "Important", icon: "stop", color: rgb("#f74242"))

// Proof box
#let proof-box = custom-box.with(title: context states.localization.get().proof, icon: "report", color: eastern)

// Question box
#let question-box = custom-box.with(title: "Question", icon: "question", color: purple)
