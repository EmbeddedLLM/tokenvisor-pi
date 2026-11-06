// Pi AI delegates are cached across fixture package roots in Bun. Every fake
// package must therefore use the same observation and response contract.
export const PI_AI_DELEGATE_SOURCE = `
export function streamSimple(model, context, options) {
  globalThis.__PI_TEST_CAPTURED_OPTIONS__ = options;
  globalThis.__PI_TEST_CAPTURED_CONTEXT__ = context;
  globalThis.__PI_TEST_CAPTURED_REQUEST__ = { model, context, options };
  globalThis.__PI_TEST_CAPTURED_CALLS__ = (globalThis.__PI_TEST_CAPTURED_CALLS__ || 0) + 1;
  const terminal = globalThis.__PI_TEST_STREAM_RESULT__;
  return { result: () => Promise.resolve(terminal) };
}
`;

export function resetPiAiDelegate(): void {
  delete (globalThis as any).__PI_TEST_STREAM_RESULT__;
  delete (globalThis as any).__PI_TEST_CAPTURED_OPTIONS__;
  delete (globalThis as any).__PI_TEST_CAPTURED_CONTEXT__;
  delete (globalThis as any).__PI_TEST_CAPTURED_REQUEST__;
  delete (globalThis as any).__PI_TEST_CAPTURED_CALLS__;
}
