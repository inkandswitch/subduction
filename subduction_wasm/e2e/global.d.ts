import type * as Subduction from "../dist/wrapper/index";

declare global {
  interface Window {
    subduction: typeof Subduction;
    subductionReady: boolean;
  }
}
