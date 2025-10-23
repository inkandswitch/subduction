import { test, expect } from "@playwright/test";
import { URL } from "./config";

test.beforeEach(async ({ page }) => {
  await page.goto(URL);
  await page.waitForFunction(() => !!window.subduction);
});

test.describe("Subduction", async () => {
   test("constructor", async ({ page }) => {
     const out = await page.evaluate(async () => {
       const { Subduction } = window.subduction;
         const syncer = new Subduction();
         return { syncer };
     });

     expect(out.syncer).toBeDefined();
   });
});
