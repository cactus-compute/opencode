import { MsEdgeTTS, OUTPUT_FORMAT } from "msedge-tts"
import { Log } from "../util/log"
// Config is lazy-imported to break circular dependency (config imports TTS.ConfigSchema)
import { BusEvent } from "../bus/bus-event"
import { Bus } from "../bus"
import { Global } from "../global"
import z from "zod"
import path from "path"
import fs from "fs/promises"
import { Readable } from "stream"
import { cleanTextForSpeech } from "@opencode-ai/util/text"

export namespace TTS {
  const log = Log.create({ service: "tts" })

  // Microsoft Edge TTS voice options - high quality neural voices
  export const Voice = z.enum([
    // English (US)
    "en-US-AvaNeural",
    "en-US-AvaMultilingualNeural",
    "en-US-AriaNeural",
    "en-US-JennyNeural",
    "en-US-GuyNeural",
    "en-US-ChristopherNeural",
    "en-US-EricNeural",
    "en-US-MichelleNeural",
    "en-US-RogerNeural",
    "en-US-SteffanNeural",
    // English (UK)
    "en-GB-SoniaNeural",
    "en-GB-RyanNeural",
    "en-GB-LibbyNeural",
    // English (Australia)
    "en-AU-NatashaNeural",
    "en-AU-WilliamNeural",
    // Other popular voices
    "en-IE-EmilyNeural",
    "en-CA-ClaraNeural",
    "en-CA-LiamNeural",
    "en-IN-NeerjaNeural",
    "en-IN-PrabhatNeural",
  ])
  export type Voice = z.infer<typeof Voice>

  export const ConfigSchema = z
    .object({
      enabled: z.boolean().optional().default(true).describe("Enable text-to-speech for assistant responses"),
      voice: Voice.optional()
        .default("en-US-AvaNeural")
        .describe("Voice to use for TTS (Microsoft Edge neural voices)"),
      rate: z
        .string()
        .optional()
        .default("default")
        .describe("Speech rate adjustment (e.g., '+10%', '-20%', 'default')"),
      volume: z.string().optional().default("default").describe("Volume adjustment (e.g., '+10%', '-20%', 'default')"),
      pitch: z.string().optional().default("default").describe("Pitch adjustment (e.g., '+10Hz', '-20Hz', 'default')"),
    })
    .strict()
    .meta({
      ref: "TTSConfig",
    })
  export type ConfigType = z.infer<typeof ConfigSchema>

  export const Event = {
    SpeakStart: BusEvent.define(
      "tts.speak.start",
      z.object({
        sessionID: z.string(),
        messageID: z.string(),
        partID: z.string(),
      }),
    ),
    SpeakEnd: BusEvent.define(
      "tts.speak.end",
      z.object({
        sessionID: z.string(),
        messageID: z.string(),
        partID: z.string(),
      }),
    ),
    SpeakError: BusEvent.define(
      "tts.speak.error",
      z.object({
        sessionID: z.string(),
        messageID: z.string().optional(),
        partID: z.string().optional(),
        error: z.string(),
      }),
    ),
    EnabledChanged: BusEvent.define(
      "tts.enabled.changed",
      z.object({
        enabled: z.boolean(),
      }),
    ),
  }

  // Runtime state
  let enabled = false
  let speaking = false
  let proc: ReturnType<typeof Bun.spawn> | undefined
  let queue: Array<{
    text: string
    sessionID: string
    messageID: string
    partID: string
  }> = []

  export function isEnabled() {
    return enabled
  }

  export function isSpeaking() {
    return speaking
  }

  export async function init() {
    try {
      const { Config } = await import("../config/config");
      const config = await Config.get()
      enabled = config.tts?.enabled ?? false
      log.info("initialized", { enabled })
    } catch (err) {
      log.error("failed to initialize", { error: err })
      enabled = false
    }
  }

  export function toggle() {
    enabled = !enabled
    log.info("toggled", { enabled })
    Bus.publish(Event.EnabledChanged, { enabled })
    return enabled
  }

  export function enable() {
    enabled = true
    log.info("enabled")
    Bus.publish(Event.EnabledChanged, { enabled })
  }

  export function disable() {
    enabled = false
    stop()
    log.info("disabled")
    Bus.publish(Event.EnabledChanged, { enabled })
  }

  export function stop() {
    if (proc) {
      try {
        proc.kill()
      } catch {}
      proc = undefined
    }
    queue = []
    speaking = false
  }

  export function speak(input: { text: string; sessionID: string; messageID: string; partID: string }) {
    if (!enabled) return

    // Clean up the text for TTS
    const cleaned = cleanTextForSpeech(input.text)
    if (!cleaned.trim()) return

    queue.push({ ...input, text: cleaned })
    // Don't await - let it process in background to avoid blocking
    processQueue().catch((err) => log.error("processQueue failed", { error: err }))
  }

  export async function synthesize(input: {
    text: string
    voice?: string
    rate?: string
    volume?: string
    pitch?: string
  }): Promise<ReadableStream<Uint8Array>> {
    const { Config } = await import("../config/config");
    const config = await Config.get()
    const cfg = config.tts
    const tts = new MsEdgeTTS()
    await tts.setMetadata(
      input.voice ?? cfg?.voice ?? "en-US-AriaNeural",
      OUTPUT_FORMAT.AUDIO_24KHZ_96KBITRATE_MONO_MP3,
    )
    const cleaned = cleanTextForSpeech(input.text)
    const stream = tts.toStream(cleaned, {
      rate: input.rate ?? cfg?.rate ?? "default",
      volume: input.volume ?? cfg?.volume ?? "default",
      pitch: input.pitch ?? cfg?.pitch ?? "default",
    })
    return Readable.toWeb(stream.audioStream) as unknown as ReadableStream<Uint8Array>
  }

  async function processQueue() {
    if (speaking || queue.length === 0) return

    speaking = true
    const item = queue.shift()!

    try {
      Bus.publish(Event.SpeakStart, {
        sessionID: item.sessionID,
        messageID: item.messageID,
        partID: item.partID,
      })

      const { Config } = await import("../config/config");
      const config = await Config.get()
      const ttsConfig = config.tts

      const tts = new MsEdgeTTS()
      await tts.setMetadata(ttsConfig?.voice ?? "en-US-AriaNeural", OUTPUT_FORMAT.AUDIO_24KHZ_96KBITRATE_MONO_MP3)

      // Create temp file for audio
      const dir = path.join(Global.Path.data, "tts")
      await fs.mkdir(dir, { recursive: true })
      const file = await tts.toFile(dir, item.text, {
        rate: ttsConfig?.rate ?? "default",
        volume: ttsConfig?.volume ?? "default",
        pitch: ttsConfig?.pitch ?? "default",
      })

      // Play the audio using platform-specific command
      await playAudio(file.audioFilePath)

      // Cleanup temp file
      await fs.unlink(file.audioFilePath).catch(() => {})

      Bus.publish(Event.SpeakEnd, {
        sessionID: item.sessionID,
        messageID: item.messageID,
        partID: item.partID,
      })
    } catch (error) {
      log.error("speak failed", { error })
      Bus.publish(Event.SpeakError, {
        sessionID: item.sessionID,
        messageID: item.messageID,
        partID: item.partID,
        error: error instanceof Error ? error.message : String(error),
      })
    } finally {
      speaking = false
      proc = undefined
      // Process next item in queue (use setImmediate to avoid stack buildup)
      if (queue.length > 0) {
        setImmediate(() => {
          processQueue().catch((err) => log.error("processQueue failed", { error: err }))
        })
      }
    }
  }

  async function playAudio(filepath: string): Promise<void> {
    const platform = process.platform

    return new Promise((resolve, reject) => {
      let command: string[]

      if (platform === "darwin") {
        // macOS - use afplay
        command = ["afplay", filepath]
      } else if (platform === "win32") {
        // Windows - use PowerShell to play audio
        const safePath = filepath.replace(/'/g, "''")
        command = [
          "powershell",
          "-Command",
          `Add-Type -AssemblyName presentationCore; $player = New-Object System.Windows.Media.MediaPlayer; $player.Open('${safePath}'); $player.Play(); Start-Sleep -Seconds ([math]::Ceiling($player.NaturalDuration.TimeSpan.TotalSeconds + 1)); $player.Close()`,
        ]
      } else {
        // Linux - try mpv, then ffplay, then aplay
        // mpv is recommended by edge-tts
        command = ["mpv", "--no-video", "--really-quiet", filepath]
      }

      log.debug("playing audio", { command: command[0], filepath })

      proc = Bun.spawn(command, {
        stdout: "ignore",
        stderr: "ignore",
        onExit(subprocess, exitCode) {
          if (exitCode === 0) {
            resolve()
          } else {
            // If mpv fails on Linux, try ffplay
            if (platform === "linux" && command[0] === "mpv") {
              if (!speaking) {
                reject(new Error("Playback cancelled"))
                return
              }
              const ffplayProcess = Bun.spawn(["ffplay", "-nodisp", "-autoexit", "-loglevel", "quiet", filepath], {
                stdout: "ignore",
                stderr: "ignore",
                onExit(_, ffplayExitCode) {
                  if (ffplayExitCode === 0) {
                    resolve()
                  } else {
                    reject(new Error(`Failed to play audio. Install mpv or ffmpeg for TTS playback.`))
                  }
                },
              })
              proc = ffplayProcess
            } else {
              reject(new Error(`Audio playback failed with exit code ${exitCode}`))
            }
          }
        },
      })
    })
  }

  // Get list of available voices - available enum values
  export const VOICES = [
    "en-US-AvaNeural",
    "en-US-AvaMultilingualNeural",
    "en-US-AriaNeural",
    "en-US-JennyNeural",
    "en-US-GuyNeural",
    "en-US-ChristopherNeural",
    "en-US-EricNeural",
    "en-US-MichelleNeural",
    "en-US-RogerNeural",
    "en-US-SteffanNeural",
    "en-GB-SoniaNeural",
    "en-GB-RyanNeural",
    "en-GB-LibbyNeural",
    "en-AU-NatashaNeural",
    "en-AU-WilliamNeural",
    "en-IE-EmilyNeural",
    "en-CA-ClaraNeural",
    "en-CA-LiamNeural",
    "en-IN-NeerjaNeural",
    "en-IN-PrabhatNeural",
  ] as const

  export function getVoices(): Voice[] {
    return [...VOICES]
  }

  // Get default voice based on config or system
  export async function getDefaultVoice(): Promise<Voice> {
    const { Config } = await import("../config/config");
    const config = await Config.get()
    return config.tts?.voice ?? "en-US-AriaNeural"
  }
}
