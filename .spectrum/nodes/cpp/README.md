# cpp (L3)

The DuckDB MongoDB extension, held in place. The spectrum is embedded in the
extension's checkout, so `source` names the code where it lives
(`../../../src/`) rather than vendoring a copy: the extension's build closure
(the DuckDB submodule and the vcpkg dependencies) is too large to carry, and the
code the L2 actions correspond to (`src/mongo_insert.cpp`) lives in this tree.

The `// spectrum:` seams in `src/mongo_insert.cpp` are comments in the shipped
source; the conformance edge `insert-cpp` generates the trace writer and the
emits from them into a throwaway build, and instruments, builds and runs the code
there (edge README). The build overlays the patched sources onto this checkout
and rebuilds `unittest`, so nothing here carries build artifacts.

**What is identified, and what is not.** `source` covers the extension's own
`src/`. The build links against the dependencies already installed in the
checkout's `build/release/vcpkg_installed`, resolved once when the checkout was
first built, not re-resolved per run and not hashed here. A conformance record
therefore claims about this `src/` with whatever those installed libraries are,
and the DuckDB submodule at whatever revision the checkout holds. That is the
stated bound of an embedded node whose closure is not vendored.
