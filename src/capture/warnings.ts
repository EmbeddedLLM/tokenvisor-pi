type WarningSink = (message: string) => void;

export interface CaptureWarningReporter {
  report(sessionId: string, code: string, issue: string, sink: WarningSink): void;
}

export function createCaptureWarningReporter(): CaptureWarningReporter {
  const reported = new Map<string, Set<string>>();
  return {
    report(sessionId, code, issue, sink) {
      const codes = reported.get(sessionId) ?? new Set<string>();
      if (codes.has(code)) return;
      codes.add(code);
      reported.set(sessionId, codes);
      const message = code === "request-sequence"
        ? "[tokenvisor] Previous-request identity is unavailable for this request. Trace reconstruction may contain a sequence gap; export eligibility is checked separately."
        : `[tokenvisor] Capture observed ${issue}; request linkage may be incomplete.`;
      try {
        sink(message);
      } catch {
        // Capture warnings must never interrupt inference.
      }
    },
  };
}
