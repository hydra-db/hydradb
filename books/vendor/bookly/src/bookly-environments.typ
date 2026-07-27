#import "bookly-defaults.typ": *

#let front-matter(body) = context {
  set heading(numbering: none)
  set page(numbering: "i")
  states.page-numbering.update("i")
  states.num-pattern.update(none)
  states.isfrontmatter.update(true)

  if states.open-right.get() {
    counter(page).update(0)
  } else {
    counter(page).update(1)
  }

  body
}

// Main matter
#let main-matter(body) = context {
  set heading(numbering: "1.1.")

  let numbering = "1/1"
  set page(numbering: numbering)

  // states.isfrontmatter.update(false)
  states.page-numbering.update("1/1")
  states.num-heading.update("1")
  states.num-pattern.update("1.1.")
  states.num-pattern-fig.update("1.1")
  states.num-pattern-subfig.update("1.1a")
  states.num-pattern-eq.update("(1.1a)")

  if states.open-right.get() {
    counter(page).update(0)
  } else {
    counter(page).update(1)
  }

  // To be checked
  if states.isfrontmatter.get() and states.tufte.get() and states.open-right.get() {
    counter(page).update(page => page + 1)
  }

  body
}

// Back matter
#let back-matter(body) = {
  set page(header: none, footer: none)

  body
}

// Appendix
#let appendix(body) = context {
  set heading(numbering: "A.1.")

  // Reset heading counter
  counter(heading.where(level: 1)).update(0)

  // Reset heading counter for the table of contents
  counter(heading).update(0)

  // Update states for chapter function
  states.isfrontmatter.update(false)
  states.num-heading.update("A")
  states.num-pattern.update("A.1.")
  states.num-pattern-fig.update("A.1")
  states.num-pattern-subfig.update("A.1a")
  states.num-pattern-eq.update("(A.1a)")
  states.isappendix.update(true)

  body
}
