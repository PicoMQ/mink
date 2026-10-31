import { chromium } from "playwright";
import { spawn } from "node:child_process";
import { rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const FPS = 60;
const scene = new URL("./scene.html", import.meta.url);
const output = process.argv[2] ?? fileURLToPath(new URL("./mink.mp4", import.meta.url));
const audio = join(tmpdir(), `mink-${process.pid}.wav`);

const browser = await chromium.launch();
try {
  const page = await browser.newPage({ viewport: { width: 1920, height: 1080 } });
  await page.goto(scene.href);
  await page.evaluate(() => document.fonts.ready);
  const length = await page.evaluate(() => window.LENGTH);
  writeFileSync(audio, Buffer.from(await page.evaluate(() => window.soundtrack()), "base64"));

  const ffmpeg = spawn(
    "ffmpeg",
    ["-y", "-loglevel", "error", "-f", "image2pipe", "-framerate", String(FPS), "-i", "-", "-i", audio,
     "-map", "0:v", "-map", "1:a", "-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", "16", "-preset", "slow",
     "-c:a", "aac", "-b:a", "192k", "-shortest", output],
    { stdio: ["pipe", "inherit", "inherit"] },
  );
  const encoded = new Promise((resolve, reject) =>
    ffmpeg.on("close", (code) => (code === 0 ? resolve() : reject(new Error(`ffmpeg exited with ${code}`)))));

  for (let frame = 0; frame < Math.round(length * FPS); frame++) {
    await page.evaluate((t) => window.render(t), frame / FPS);
    if (!ffmpeg.stdin.write(await page.screenshot())) {
      await new Promise((resolve) => ffmpeg.stdin.once("drain", resolve));
    }
  }

  ffmpeg.stdin.end();
  await encoded;
  console.log(output);
} finally {
  await browser.close();
  rmSync(audio, { force: true });
}
