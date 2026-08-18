---- MODULE Trace ----
\* Placeholder record module so Replay parses. The conformance runner replaces
\* its contents with one recorded trace per replay, so it is declared in the
\* ledger but not hashed.
EXTENDS Naturals, Sequences, TLC
TraceLog == << [action |-> "Init"] >>
====
