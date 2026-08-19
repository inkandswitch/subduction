import type * as Subduction from "../pkg/subduction_wasm";

declare global {
  interface Window {
    subduction: typeof Subduction;
    subductionReady: boolean;
  }
}
