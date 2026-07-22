#import "template.typ": *

#show: book.with(
  title: "Verifying TurboLay",
  subtitle: "Quint, formal verification, and model-based testing",
)

#part([Verifying TurboLay])

#include "chapters/quint-00-correctness-problem.typ"
#include "chapters/quint-01-quint-from-zero.typ"
#include "chapters/quint-02-first-model-m1.typ"
#include "chapters/quint-03-invariants-and-buggy-twin.typ"
#include "chapters/quint-04-deterministic-scenarios.typ"
#include "chapters/quint-05-model-gallery.typ"
#include "chapters/quint-06-bounded-proof-apalache.typ"
#include "chapters/quint-07-rust-mbt.typ"
#include "chapters/quint-08-assurance-stack.typ"
