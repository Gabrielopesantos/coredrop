/* A deliberately faulting workload for the Layer 3 live smoke test.
 *
 * Dereferences a null pointer, dying by SIGSEGV (signal 11). When run inside a
 * container on the smoke VM, the kernel routes the fault to coredrop's
 * core_pattern handler, producing a real core + /proc snapshot + manifest in
 * the bucket. Run as a non-init child (see workloads/segfault.yaml) so the
 * synchronous fatal signal is never the namespace-init special case.
 *
 * `volatile` keeps the optimizer from eliding the store.
 */
int main(void) {
    volatile int *p = 0;
    *p = 42;
    return 0;
}
